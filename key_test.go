package datastore_test

import (
	"bytes"
	"path"
	"strings"
	"testing"

	. "github.com/ipfs/go-datastore"
	"github.com/stretchr/testify/require"
)

func subtestKey(t *testing.T, s string) {
	fixed := path.Clean("/" + s)
	namespaces := strings.Split(fixed, "/")[1:]
	lastNamespace := namespaces[len(namespaces)-1]
	lnparts := strings.Split(lastNamespace, ":")
	ktype := ""
	if len(lnparts) > 1 {
		ktype = strings.Join(lnparts[:len(lnparts)-1], ":")
	}
	kname := lnparts[len(lnparts)-1]

	kchild := path.Clean(fixed + "/cchildd")
	kparent := "/" + strings.Join(namespaces[:len(namespaces)-1], "/")
	kpath := path.Clean(kparent + "/" + ktype)
	kinstance := fixed + ":" + "inst"

	t.Log("Testing: ", NewKey(s))

	require.Equal(t, fixed, NewKey(s).String())
	require.Equal(t, NewKey(s), NewKey(s))
	require.Equal(t, NewKey(s).String(), NewKey(s).String())
	require.Equal(t, kname, NewKey(s).Name())
	require.Equal(t, ktype, NewKey(s).Type())
	require.Equal(t, kpath, NewKey(s).Path().String())
	require.Equal(t, kinstance, NewKey(s).Instance("inst").String())

	require.Equal(t, kchild, NewKey(s).Child(NewKey("cchildd")).String())
	require.Equal(t, fixed, NewKey(s).Child(NewKey("cchildd")).Parent().String())
	require.Equal(t, kchild, NewKey(s).ChildString("cchildd").String())
	require.Equal(t, fixed, NewKey(s).ChildString("cchildd").Parent().String())
	require.Equal(t, kparent, NewKey(s).Parent().String())
	require.Len(t, NewKey(s).List(), len(namespaces))
	require.Len(t, NewKey(s).Namespaces(), len(namespaces))
	for i, e := range NewKey(s).List() {
		require.Equal(t, e, namespaces[i])
	}

	require.Equal(t, NewKey(s), NewKey(s))
	require.True(t, NewKey(s).Equal(NewKey(s)))
	require.False(t, NewKey(s).Equal(NewKey("/fdsafdsa/"+s)))

	// less
	require.False(t, NewKey(s).Less(NewKey(s).Parent()))
	require.True(t, NewKey(s).Less(NewKey(s).ChildString("foo")))
}

func TestKeyBasic(t *testing.T) {
	subtestKey(t, "")
	subtestKey(t, "abcde")
	subtestKey(t, "disahfidsalfhduisaufidsail")
	subtestKey(t, "/fdisahfodisa/fdsa/fdsafdsafdsafdsa/fdsafdsa/")
	subtestKey(t, "4215432143214321432143214321")
	subtestKey(t, "/fdisaha////fdsa////fdsafdsafdsafdsa/fdsafdsa/")
	subtestKey(t, "abcde:fdsfd")
	subtestKey(t, "disahfidsalfhduisaufidsail:fdsa")
	subtestKey(t, "/fdisahfodisa/fdsa/fdsafdsafdsafdsa/fdsafdsa/:")
	subtestKey(t, "4215432143214321432143214321:")
	subtestKey(t, "fdisaha////fdsa////fdsafdsafdsafdsa/fdsafdsa/f:fdaf")
}

func TestKeyAncestry(t *testing.T) {
	k1 := NewKey("/A/B/C")
	k2 := NewKey("/A/B/C/D")
	k3 := NewKey("/AB")
	k4 := NewKey("/A")

	require.Equal(t, "/A/B/C", k1.String())
	require.Equal(t, "/A/B/C/D", k2.String())
	require.True(t, k1.IsAncestorOf(k2))
	require.True(t, k2.IsDescendantOf(k1))
	require.True(t, k4.IsAncestorOf(k2))
	require.True(t, k4.IsAncestorOf(k1))
	require.False(t, k4.IsDescendantOf(k2))
	require.False(t, k4.IsDescendantOf(k1))
	require.False(t, k3.IsDescendantOf(k4))
	require.False(t, k4.IsAncestorOf(k3))
	require.True(t, k2.IsDescendantOf(k4))
	require.True(t, k1.IsDescendantOf(k4))
	require.False(t, k2.IsAncestorOf(k4))
	require.False(t, k1.IsAncestorOf(k4))
	require.False(t, k2.IsAncestorOf(k2))
	require.False(t, k1.IsAncestorOf(k1))
	require.Equal(t, k2.String(), k1.Child(NewKey("D")).String())
	require.Equal(t, k2.String(), k1.ChildString("D").String())
	require.Equal(t, k1.String(), k2.Parent().String())
	require.Equal(t, k1.Path().String(), k2.Parent().Path().String())
}

func TestType(t *testing.T) {
	k1 := NewKey("/A/B/C:c")
	k2 := NewKey("/A/B/C:c/D:d")

	require.True(t, k1.IsAncestorOf(k2))
	require.True(t, k2.IsDescendantOf(k1))
	require.Equal(t, "C", k1.Type())
	require.Equal(t, "D", k2.Type())
	require.Equal(t, k1.Type(), k2.Parent().Type())
}

func TestRandom(t *testing.T) {
	keys := map[Key]bool{}
	for range 1000 {
		r := RandomKey()
		_, found := keys[r]
		require.False(t, found)
		keys[r] = true
	}
	require.Len(t, keys, 1000)
}

func TestLess(t *testing.T) {
	require.Less(t, "/a/b/c", "/a/b/c/d")
	require.Less(t, "/a/b", "/a/b/c/d")
	require.Less(t, "/a", "/a/b/c/d")
	require.Less(t, "/a/a/c", "/a/b/c")
	require.Less(t, "/a/a/d", "/a/b/c")
	require.Less(t, "/a/b/c/d/e/f/g/h", "/b")
	require.Less(t, "/", "/a")
}

func TestKeyMarshalJSON(t *testing.T) {
	cases := []struct {
		key  Key
		data []byte
		err  string
	}{
		{NewKey("/a/b/c"), []byte("\"/a/b/c\""), ""},
		{NewKey("/shouldescapekey\"/with/quote"), []byte("\"/shouldescapekey\\\"/with/quote\""), ""},
	}

	for i, c := range cases {
		out, err := c.key.MarshalJSON()
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d marshal error mismatch: expected: %s, got: %s", i, c.err, err)
		}
		require.Truef(t, bytes.Equal(c.data, out), "case %d value mismatch: expected: %s, got: %s", i, string(c.data), string(out))

		if c.err == "" {
			key := Key{}
			err = key.UnmarshalJSON(out)
			require.NoErrorf(t, err, "case %d error parsing key from json output", i)
			require.Truef(t, c.key.Equal(key), "case %d parsed key from json output mismatch. expected: %s, got: %s", i, c.key.String(), key.String())
		}
	}
}

func TestKeyUnmarshalJSON(t *testing.T) {
	cases := []struct {
		data []byte
		key  Key
		err  string
	}{
		{[]byte("\"/a/b/c\""), NewKey("/a/b/c"), ""},
		{[]byte{}, Key{}, "unexpected end of JSON input"},
		{[]byte{'"'}, Key{}, "unexpected end of JSON input"},
		{[]byte(`""`), NewKey(""), ""},
	}

	for i, c := range cases {
		key := Key{}
		err := key.UnmarshalJSON(c.data)
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d marshal error mismatch: expected: %s, got: %s", i, c.err, err)
		}
		require.Truef(t, key.Equal(c.key), "case %d key mismatch: expected: %s, got: %s", i, c.key, key)
	}
}

func TestKey_RootNamespace(t *testing.T) {
	tests := []struct {
		name string
		key  string
		want string
	}{
		{name: "empty path", key: "/", want: ""},
		{name: "single namespace", key: "/Comedy", want: "Comedy"},
		{name: "long path", key: "/Comedy/MontyPython/Actor:JohnCleese", want: "Comedy"},
		{name: "root + type", key: "/Comedy:MontyPython", want: "Comedy:MontyPython"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := RawKey(tt.key).RootNamespace()
			require.Equal(t, tt.want, got, "RootNamespace() returned wrong value")
		})
	}
}
