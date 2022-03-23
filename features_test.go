package datastore

import (
	"fmt"
	"reflect"
	"testing"
)

func TestFeatureByName(t *testing.T) {
	feat, ok := FeatureByName(FeatureNameBatching)
	if !ok {
		t.Fatalf("expected a batching feature")
	}
	if feat.Name != FeatureNameBatching ||
		feat.Interface != (*BatchingFeature)(nil) ||
		feat.DatastoreInterface != (*Batching)(nil) {
		t.Fatalf("expected a batching feature, got %v", feat)
	}

	feat, ok = FeatureByName("UnknownFeature")
	if ok {
		t.Fatalf("expected UnknownFeature not to be found")
	}
}

func featuresByNames(names []string) (fs []Feature) {
	for _, n := range names {
		f, ok := FeatureByName(n)
		if !ok {
			panic(fmt.Sprintf("unknown feature %s", n))
		}
		fs = append(fs, f)
	}
	return
}

func TestFeaturesForDatastore(t *testing.T) {
	cases := []struct {
		name             string
		d                Datastore
		expectedFeatures []string
	}{
		{
			name:             "MapDatastore",
			d:                &MapDatastore{},
			expectedFeatures: []string{"Batching"},
		},
		{
			name:             "NullDatastore",
			d:                &NullDatastore{},
			expectedFeatures: []string{"Batching", "Checked", "GC", "Persistent", "Scrubbed", "Transaction"},
		},
		{
			name:             "LogDatastore",
			d:                &LogDatastore{},
			expectedFeatures: []string{"Batching", "Checked", "GC", "Persistent", "Scrubbed"},
		},
		{
			name:             "nil datastore",
			d:                nil,
			expectedFeatures: nil,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			feats := FeaturesForDatastore(c.d)
			if len(feats) != len(c.expectedFeatures) {
				t.Fatalf("expected %d features, got %v", len(c.expectedFeatures), feats)
			}
			expectedFeats := featuresByNames(c.expectedFeatures)
			if !reflect.DeepEqual(expectedFeats, feats) {
				t.Fatalf("expected features %v, got %v", c.expectedFeatures, feats)
			}
		})
	}
}
