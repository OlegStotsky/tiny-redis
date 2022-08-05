package pkg

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"
)

func TestDBOpen(t *testing.T) {
	db, err := NewDB(fmt.Sprintf("%s/%s", t.TempDir(), t.Name()))
	require.NoError(t, err)

	err = db.Open()
	require.NoError(t, err)
}

func TestDBSet(t *testing.T) {
	f, err := os.CreateTemp("./", "db")
	defer func() {
		os.Remove(f.Name())
	}()

	db, err := NewDB(f.Name())
	require.NoError(t, err)

	err = db.Open()
	require.NoError(t, err)

	m := map[string]string{}

	for i := 0; i < 10000; i++ {
		k, v := rand.Int(), rand.Int()
		kStr, vStr := strconv.Itoa(k), strconv.Itoa(v)
		m[kStr] = vStr
		err = db.Set(kStr, vStr, &setOptions{})
		require.NoError(t, err)
	}

	err = db.Close()
	require.NoError(t, err)

	db, err = NewDB(f.Name())
	require.NoError(t, err)

	err = db.Open()
	require.NoError(t, err)
	defer db.Close()

	for k, v := range m {
		dbV, ok := db.Get(k)
		require.True(t, ok)
		require.Equal(t, v, dbV)
	}
}

func TestDBSetWithNX(t *testing.T) {
	f, err := os.CreateTemp("./", "db")
	defer func() {
		os.Remove(f.Name())
	}()

	db, err := NewDB(f.Name())
	require.NoError(t, err)

	err = db.Open()
	require.NoError(t, err)

	err = db.Set("foo", "bar", &setOptions{nx: true})
	require.NoError(t, err)

	val, ok := db.Get("foo")
	require.True(t, ok)
	require.Equal(t, "bar", val)

	err = db.Set("foo", "bar2", &setOptions{nx: true})
	require.NoError(t, err)

	val, ok = db.Get("foo")
	require.True(t, ok)
	require.Equal(t, "bar", val)
}

func TestDBSetWithXX(t *testing.T) {
	f, err := os.CreateTemp("./", "db")
	defer func() {
		os.Remove(f.Name())
	}()

	db, err := NewDB(f.Name())
	require.NoError(t, err)

	err = db.Open()
	require.NoError(t, err)

	err = db.Set("foo", "bar", &setOptions{xx: true})
	require.NoError(t, err)

	val, ok := db.Get("foo")
	require.False(t, ok)

	err = db.Set("foo", "bar", &setOptions{})
	require.NoError(t, err)

	val, ok = db.Get("foo")
	require.True(t, ok)
	require.Equal(t, "bar", val)

	err = db.Set("foo", "bar2", &setOptions{xx: true})
	require.NoError(t, err)

	val, ok = db.Get("foo")
	require.True(t, ok)
	require.Equal(t, "bar2", val)
}

func TestDBSetWithTimeout(t *testing.T) {
	f, err := os.CreateTemp("./", "db")
	defer func() {
		os.Remove(f.Name())
	}()

	db, err := NewDB(f.Name())
	require.NoError(t, err)

	err = db.Open()
	require.NoError(t, err)

	err = db.Set("foo", "bar", &setOptions{ttl: time.Now().Add(1 * time.Second)})
	require.NoError(t, err)

	val, ok := db.Get("foo")
	require.True(t, ok)
	require.Equal(t, "bar", val)

	time.Sleep(1 * time.Second)

	_, ok = db.Get("foo")
	require.False(t, ok)
}

func TestDBDelete(t *testing.T) {
	f, err := os.CreateTemp("./", "db")
	defer func() {
		os.Remove(f.Name())
	}()

	db, err := NewDB(f.Name())
	require.NoError(t, err)

	err = db.Open()
	require.NoError(t, err)

	err = db.Set("foo", "bar", &setOptions{})
	require.NoError(t, err)

	err, ok := db.Delete("foo")
	require.NoError(t, err)
	require.True(t, ok)

	_, ok = db.Get("foo")
	require.False(t, ok)

	err = db.Close()
	require.NoError(t, err)

	db, err = NewDB(f.Name())
	require.NoError(t, err)

	err = db.Open()
	require.NoError(t, err)
	defer db.Close()

	_, ok = db.Get("foo")
	require.False(t, ok)
}
