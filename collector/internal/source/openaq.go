package source

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"airquality/collector/internal/model"
)

// OpenAQSource reads variant 20's real data source — the OpenAQ v3 API
// (https://docs.openaq.org/). A key is required (header X-API-Key). Because the
// public API is rate-limited and may be unreachable in CI, docker-compose
// defaults to the synthetic source; set SOURCE=openaq + OPENAQ_API_KEY to use
// this one.
type OpenAQSource struct {
	apiKey   string
	client   *http.Client
	base     string
	locIDs   []int // OpenAQ numeric location ids to poll
	stations []model.Station
}

// NewOpenAQ constructs the client. locationIDs are OpenAQ location ids; if
// empty, a default set of well-known city stations is used.
func NewOpenAQ(apiKey string, locationIDs []int) *OpenAQSource {
	if len(locationIDs) == 0 {
		locationIDs = []int{2178, 8118, 155, 1236, 4146, 7936} // sample global stations
	}
	return &OpenAQSource{
		apiKey:  apiKey,
		client:  &http.Client{Timeout: 15 * time.Second},
		base:    "https://api.openaq.org/v3",
		locIDs:  locationIDs,
	}
}

func (o *OpenAQSource) Name() string { return "openaq" }

type oaLocationResp struct {
	Results []struct {
		ID          int    `json:"id"`
		Name        string `json:"name"`
		Country     struct{ Code string `json:"code"` } `json:"country"`
		Locality    string  `json:"locality"`
		Coordinates struct {
			Latitude  float64 `json:"latitude"`
			Longitude float64 `json:"longitude"`
		} `json:"coordinates"`
	} `json:"results"`
}

func (o *OpenAQSource) Stations(ctx context.Context) ([]model.Station, error) {
	if o.stations != nil {
		return o.stations, nil
	}
	for _, id := range o.locIDs {
		var resp oaLocationResp
		if err := o.get(ctx, fmt.Sprintf("/locations/%d", id), &resp); err != nil {
			return nil, err
		}
		for _, r := range resp.Results {
			o.stations = append(o.stations, model.Station{
				ID:        "oa-" + strconv.Itoa(r.ID),
				Name:      r.Name,
				City:      r.Locality,
				Country:   r.Country.Code,
				Latitude:  r.Coordinates.Latitude,
				Longitude: r.Coordinates.Longitude,
			})
		}
	}
	return o.stations, nil
}

type oaLatestResp struct {
	Results []struct {
		Parameter struct {
			Name  string `json:"name"`
			Units string `json:"units"`
		} `json:"parameter"`
		Value    float64 `json:"value"`
		Datetime struct {
			UTC string `json:"utc"`
		} `json:"datetime"`
	} `json:"results"`
}

func (o *OpenAQSource) Fetch(ctx context.Context, ids []string) ([]model.Measurement, error) {
	stations, err := o.Stations(ctx)
	if err != nil {
		return nil, err
	}
	byID := make(map[string]model.Station, len(stations))
	for _, s := range stations {
		byID[s.ID] = s
	}

	var out []model.Measurement
	for _, id := range ids {
		st, ok := byID[id]
		if !ok {
			continue
		}
		numericID := id[len("oa-"):]
		var resp oaLatestResp
		path := fmt.Sprintf("/locations/%s/latest", numericID)
		if err := o.get(ctx, path, &resp); err != nil {
			return nil, err
		}
		for _, r := range resp.Results {
			ts, _ := time.Parse(time.RFC3339, r.Datetime.UTC)
			out = append(out, model.Measurement{
				StationID: st.ID, Station: st.Name, City: st.City, Country: st.Country,
				Parameter: r.Parameter.Name, Value: r.Value, Unit: r.Parameter.Units,
				Latitude: st.Latitude, Longitude: st.Longitude, Timestamp: ts,
			})
		}
	}
	return out, nil
}

func (o *OpenAQSource) get(ctx context.Context, path string, v any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, o.base+path, nil)
	if err != nil {
		return err
	}
	if o.apiKey != "" {
		req.Header.Set("X-API-Key", o.apiKey)
	}
	resp, err := o.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("openaq %s: status %d", path, resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(v)
}
