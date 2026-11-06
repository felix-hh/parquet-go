package parquet_test

import (
	"fmt"
	"io"
	"math/rand"
	"slices"
	"testing"

	"github.com/parquet-go/parquet-go"
)

// clusteredStrings returns n values drawn from k distinct strings, in runs
// of runLen equal values. The values of a run share one string unless
// copies is true, which gives each value its own allocation.
func clusteredStrings(n, k, runLen int, copies bool) []string {
	prng := rand.New(rand.NewSource(1))
	distinct := make([]string, k)
	for i := range distinct {
		distinct[i] = generateString(prng, 12)
	}
	values := make([]string, n)
	for i := range values {
		v := distinct[(i/runLen)%k]
		if copies {
			v = string([]byte(v))
		}
		values[i] = v
	}
	return values
}

func newByteArrayDictionary() parquet.Dictionary {
	return parquet.ByteArrayType.NewDictionary(0, 0, parquet.ByteArrayType.NewValues(nil, nil))
}

func valuesOf(values []string) []parquet.Value {
	vals := make([]parquet.Value, len(values))
	for i, v := range values {
		vals[i] = parquet.ValueOf(v)
	}
	return vals
}

func TestByteArrayDictionaryInsertRuns(t *testing.T) {
	// Runs, a value revisited after another, and equal values in separate
	// allocations must get the same indexes as inserting one at a time.
	for _, copies := range []bool{false, true} {
		values := append(clusteredStrings(1000, 7, 5, copies), "a", "a", "b", "a", "", "", "a")
		vals := valuesOf(values)

		batch, single := newByteArrayDictionary(), newByteArrayDictionary()
		got := make([]int32, len(vals))
		batch.Insert(got, vals)
		want := make([]int32, len(vals))
		for i := range vals {
			single.Insert(want[i:i+1], vals[i:i+1])
		}

		if !slices.Equal(got, want) {
			t.Fatalf("copies=%t: indexes differ from one-at-a-time insert", copies)
		}
		if batch.Len() != single.Len() {
			t.Fatalf("copies=%t: dictionary has %d values, want %d", copies, batch.Len(), single.Len())
		}
	}
}

func TestByteArrayDictionaryBoundsRuns(t *testing.T) {
	values := clusteredStrings(1000, 50, 7, false)
	dict := newByteArrayDictionary()
	indexes := make([]int32, len(values))
	dict.Insert(indexes, valuesOf(values))

	check := func(n int) {
		min, max := dict.Bounds(indexes[:n])
		wantMin, wantMax := slices.Min(values[:n]), slices.Max(values[:n])
		if string(min.ByteArray()) != wantMin || string(max.ByteArray()) != wantMax {
			t.Fatalf("n=%d: bounds = (%q, %q), want (%q, %q)", n, min.ByteArray(), max.ByteArray(), wantMin, wantMax)
		}
	}
	// Bounds looks values up in batches of 64: cross the first batch
	// boundaries, then check the whole page.
	for n := 1; n <= 130; n++ {
		check(n)
	}
	check(len(values))
}

// BenchmarkByteArrayDictionaryBounds runs Bounds over a page of indexes in
// random order (run=1) and in runs of equal values.
func BenchmarkByteArrayDictionaryBounds(b *testing.B) {
	for _, runLen := range []int{1, 16, 256} {
		values := clusteredStrings(100_000, 1000, runLen, false)
		if runLen == 1 {
			rand.New(rand.NewSource(2)).Shuffle(len(values), func(i, j int) { values[i], values[j] = values[j], values[i] })
		}
		dict := newByteArrayDictionary()
		indexes := make([]int32, len(values))
		dict.Insert(indexes, valuesOf(values))
		b.Run(fmt.Sprintf("run=%d", runLen), func(b *testing.B) {
			for b.Loop() {
				dict.Bounds(indexes)
			}
		})
	}
}

type clusteredStringRow struct {
	Code  string  `parquet:"code,dict"`
	Value float64 `parquet:"value"`
}

// BenchmarkWriterClusteredDictionary writes a dictionary-encoded string
// column whose values arrive in runs, as in a file sorted by that column.
func BenchmarkWriterClusteredDictionary(b *testing.B) {
	for _, runLen := range []int{1, 16, 256} {
		values := clusteredStrings(100_000, 1000, runLen, false)
		rows := make([]clusteredStringRow, len(values))
		for i, v := range values {
			rows[i] = clusteredStringRow{Code: v, Value: float64(i)}
		}
		b.Run(fmt.Sprintf("run=%d", runLen), func(b *testing.B) {
			w := parquet.NewGenericWriter[clusteredStringRow](io.Discard)
			benchmarkRowsPerSecond(b, func() int {
				if _, err := w.Write(rows); err != nil {
					b.Fatal(err)
				}
				if err := w.Flush(); err != nil {
					b.Fatal(err)
				}
				return len(rows)
			})
		})
	}
}
