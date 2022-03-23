package scoped

import (
	"fmt"
	"reflect"
	"testing"

	ds "github.com/ipfs/go-datastore"
)

func featuresByNames(names ...string) (fs []ds.Feature) {
	for _, n := range names {
		f, ok := ds.FeatureByName(n)
		if !ok {
			panic(fmt.Sprintf("unknown feature %s", n))
		}
		fs = append(fs, f)
	}
	return
}

func TestWithFeatures(t *testing.T) {
	cases := []struct {
		name     string
		dstore   ds.Datastore
		features []ds.Feature

		expectedFeatures []ds.Feature
	}{
		{
			name:             "no features should return a base datastore",
			dstore:           &ds.MapDatastore{},
			features:         nil,
			expectedFeatures: nil,
		},
		{
			name:             "identity case",
			dstore:           &ds.MapDatastore{},
			features:         featuresByNames("Batching"),
			expectedFeatures: featuresByNames("Batching"),
		},
		{
			name:             "should scope down correctly",
			dstore:           &ds.LogDatastore{},
			features:         featuresByNames("Batching"),
			expectedFeatures: featuresByNames("Batching"),
		},
		{
			name:             "takes intersection of features",
			dstore:           &ds.MapDatastore{},
			features:         featuresByNames("Batching", "Checked"),
			expectedFeatures: featuresByNames("Batching"),
		},
		{
			name:             "uses correct impl even if features are not canonically sorted",
			dstore:           &ds.NullDatastore{},
			features:         featuresByNames("Checked", "Batching", "Scrubbed"),
			expectedFeatures: featuresByNames("Batching", "Checked", "Scrubbed"),
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			new := WithFeatures(c.dstore, c.features)
			newFeats := ds.FeaturesForDatastore(new)
			if len(newFeats) != len(c.expectedFeatures) {
				t.Fatalf("expected %d features, got %v", len(c.expectedFeatures), newFeats)
			}
			if !reflect.DeepEqual(newFeats, c.expectedFeatures) {
				t.Fatalf("expected features %v, got %v", c.expectedFeatures, newFeats)
			}
		})
	}
}
