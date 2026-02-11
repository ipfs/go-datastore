package scoped

import (
	ds "github.com/ipfs/go-datastore"
)

func getFeatureSet(dstore ds.Datastore) map[any]bool {
	features := ds.FeaturesForDatastore(dstore)
	featureSet := map[any]bool{}
	for _, f := range features {
		featureSet[f.Interface] = true
	}
	return featureSet
}

// Wrap returns a datastore based on the source, whose concrete type is scoped down to only the features supported by the target.
func Wrap(source ds.Datastore, target ds.Datastore) ds.Datastore {
	if source == nil || target == nil {
		return nil
	}
	return WithFeatures(source, ds.FeaturesForDatastore(target))
}

// WithFeatures returns a wrapped datastore that implements the intersection of the given datastore's features with the provided features.
func WithFeatures(dstore ds.Datastore, features []ds.Feature) ds.Datastore {
	dstoreFeatures := getFeatureSet(dstore)

	dstoreFeatureSet := map[string]bool{}
	for _, f := range features {
		if _, ok := dstoreFeatures[f.Interface]; ok {
			dstoreFeatureSet[f.Name] = true
		}
	}

	var ctor uint
	for i, f := range ds.Features() {
		if _, ok := dstoreFeatureSet[f.Name]; ok {
			ctor |= (1 << uint(i))
		}
	}
	return ctors[ctor](dstore)
}
