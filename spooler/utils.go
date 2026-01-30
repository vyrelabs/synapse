// Copyright 2025-2026 Ritvik Gupta
// SPDX-License-Identifier: Apache-2.0

package spooler

import (
	"io/fs"
	"os"
)

const filePermissions fs.FileMode = 0o770

func mkdirIfNotExists(path string) error {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		if err := os.MkdirAll(path, filePermissions); err != nil {
			return err
		}
	}
	return nil
}
