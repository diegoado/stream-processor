//go:build integration

package suite_utils

import (
	"path/filepath"
	"runtime"
)

// ProjectRoot returns the absolute path to the project root directory.
func ProjectRoot() string {
	_, filename, _, _ := runtime.Caller(0) //nolint:dogsled
	// utilities/ -> testsuite/ -> integration_test/ -> project root
	return filepath.Join(filepath.Dir(filename), "..", "..", "..")
}
