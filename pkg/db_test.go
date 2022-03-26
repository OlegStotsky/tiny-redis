package pkg

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"math/rand"
	"os"
	"strconv"
	"testing"
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
		err = db.Set(kStr, vStr)
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
		dbV, err := db.Get(k)
		require.NoError(t, err)
		require.Equal(t, v, dbV)
	}
}
