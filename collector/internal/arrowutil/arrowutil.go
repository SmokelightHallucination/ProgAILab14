// Package arrowutil builds Apache Arrow records from window aggregates. The
// same schema feeds both the Parquet sink and the Arrow Flight server
// (advanced task #3), so Go and Python exchange columnar data with zero JSON
// in between.
package arrowutil

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"airquality/collector/internal/model"
)

// Schema is the columnar layout for an aggregate batch.
var Schema = arrow.NewSchema([]arrow.Field{
	{Name: "location_id", Type: arrow.BinaryTypes.String},
	{Name: "location", Type: arrow.BinaryTypes.String},
	{Name: "city", Type: arrow.BinaryTypes.String},
	{Name: "country", Type: arrow.BinaryTypes.String},
	{Name: "parameter", Type: arrow.BinaryTypes.String},
	{Name: "unit", Type: arrow.BinaryTypes.String},
	{Name: "latitude", Type: arrow.PrimitiveTypes.Float64},
	{Name: "longitude", Type: arrow.PrimitiveTypes.Float64},
	{Name: "window_start", Type: arrow.FixedWidthTypes.Timestamp_us},
	{Name: "window_end", Type: arrow.FixedWidthTypes.Timestamp_us},
	{Name: "count", Type: arrow.PrimitiveTypes.Int64},
	{Name: "sum", Type: arrow.PrimitiveTypes.Float64},
	{Name: "avg", Type: arrow.PrimitiveTypes.Float64},
	{Name: "min", Type: arrow.PrimitiveTypes.Float64},
	{Name: "max", Type: arrow.PrimitiveTypes.Float64},
	{Name: "aqi", Type: arrow.PrimitiveTypes.Int32},
	{Name: "aqi_category", Type: arrow.BinaryTypes.String},
}, nil)

// BuildRecord turns a slice of aggregates into an Arrow record. The caller owns
// the returned record and must call Release on it.
func BuildRecord(aggs []model.Aggregate, mem memory.Allocator) arrow.Record {
	b := array.NewRecordBuilder(mem, Schema)
	defer b.Release()

	for _, a := range aggs {
		b.Field(0).(*array.StringBuilder).Append(a.StationID)
		b.Field(1).(*array.StringBuilder).Append(a.Station)
		b.Field(2).(*array.StringBuilder).Append(a.City)
		b.Field(3).(*array.StringBuilder).Append(a.Country)
		b.Field(4).(*array.StringBuilder).Append(a.Parameter)
		b.Field(5).(*array.StringBuilder).Append(a.Unit)
		b.Field(6).(*array.Float64Builder).Append(a.Latitude)
		b.Field(7).(*array.Float64Builder).Append(a.Longitude)
		b.Field(8).(*array.TimestampBuilder).Append(arrow.Timestamp(a.WindowStart.UnixMicro()))
		b.Field(9).(*array.TimestampBuilder).Append(arrow.Timestamp(a.WindowEnd.UnixMicro()))
		b.Field(10).(*array.Int64Builder).Append(a.Count)
		b.Field(11).(*array.Float64Builder).Append(a.Sum)
		b.Field(12).(*array.Float64Builder).Append(a.Avg)
		b.Field(13).(*array.Float64Builder).Append(a.Min)
		b.Field(14).(*array.Float64Builder).Append(a.Max)
		b.Field(15).(*array.Int32Builder).Append(int32(a.AQI))
		b.Field(16).(*array.StringBuilder).Append(a.Category)
	}
	return b.NewRecord()
}
