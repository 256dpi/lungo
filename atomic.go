package lungo

import (
	"fmt"
	"io"
	"os"
)

// AtomicWriteFile reads from r and writes to the file named by path. To ensure
// atomicity the contents are written to a temporary file that is linked to the
// location after being successfully written.
func AtomicWriteFile(path string, r io.Reader, mode os.FileMode) error {
	// check path
	if path == "" {
		return fmt.Errorf("empty file path")
	}

	// set default mode
	if mode == 0 {
		mode = 0666
	}

	// calculate temporary file
	tempPath := path + ".tmp"

	// delete existing file (might not exist)
	err := os.Remove(tempPath)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove existing temporary file %q: %v", tempPath, err)
	}

	// open temporary file
	tempFile, err := os.OpenFile(tempPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to create new temporary file %q: %v", tempPath, err)
	}

	// ensure temporary file is closed and deleted
	defer func() {
		_ = tempFile.Close()
		_ = os.Remove(tempPath)
	}()

	// write to temporary file
	_, err = io.Copy(tempFile, r)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to write temporary file %q: %v", tempPath, err)
	}

	// sync temporary file
	err = tempFile.Sync()
	if err != nil {
		return fmt.Errorf("failed to sync temporary file %q: %v", tempPath, err)
	}

	// close temporary file
	err = tempFile.Close()
	if err != nil {
		return fmt.Errorf("failed to close temporary file %q: %v", tempPath, err)
	}

	// rename temporary file
	err = os.Rename(tempPath, path)
	if err != nil {
		return fmt.Errorf("failed to rename temporary file from %q to %q: %v", tempPath, path, err)
	}

	return nil
}
