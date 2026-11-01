package registry

import (
	// Default catalogs.
	_ "github.com/apache/iceberg-go/catalog/hive"
	_ "github.com/apache/iceberg-go/catalog/rest"

	// Default object store.
	_ "github.com/apache/iceberg-go/io/gocloud/s3"
)
