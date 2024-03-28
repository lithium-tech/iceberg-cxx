#pragma once

#include <string_view>

namespace iceberg {

constexpr std::string_view kManifestListSchemaJson = R"EOF(
{
    "type": "record",
    "name": "manifest_file",
    "fields": [
        {
            "name": "manifest_path",
            "type": "string",
            "doc": "Location URI with FS scheme"
        },
        {
            "name": "manifest_length",
            "type": "long",
            "doc": "Total file size in bytes"
        },
        {
            "name": "partition_spec_id",
            "type": "int",
            "doc": "Spec ID used to write"
        },
        {
            "name": "content",
            "type": "int",
            "doc": "Contents of the manifest: 0=data, 1=deletes"
        },
        {
            "name": "sequence_number",
            "type": "long",
            "doc": "Sequence number when the manifest was added"
        },
        {
            "name": "min_sequence_number",
            "type": "long",
            "doc": "Lowest sequence number in the manifest"
        },
        {
            "name": "added_snapshot_id",
            "type": "long",
            "doc": "Snapshot ID that added the manifest"
        },
        {
            "name": "added_files_count",
            "type": "int",
            "doc": "Added entry count"
        },
        {
            "name": "existing_files_count",
            "type": "int",
            "doc": "Existing entry count"
        },
        {
            "name": "deleted_files_count",
            "type": "int",
            "doc": "Deleted entry count"
        },
        {
            "name": "added_rows_count",
            "type": "long",
            "doc": "Added rows count"
        },
        {
            "name": "existing_rows_count",
            "type": "long",
            "doc": "Existing rows count"
        },
        {
            "name": "deleted_rows_count",
            "type": "long",
            "doc": "Deleted rows count"
        },
        {
            "name": "partitions",
            "type": [
                "null",
                {
                    "type": "array",
                    "items": {
                        "type": "record",
                        "name": "r508",
                        "fields": [
                            {
                                "name": "contains_null",
                                "type": "boolean",
                                "doc": "True if any file has a null partition value"
                            },
                            {
                                "name": "contains_nan",
                                "type": [
                                    "null",
                                    "boolean"
                                ],
                                "default": null
                            },
                            {
                                "name": "lower_bound",
                                "type": [
                                    "null",
                                    "bytes"
                                ],
                                "default": null
                            },
                            {
                                "name": "upper_bound",
                                "type": [
                                    "null",
                                    "bytes"
                                ],
                                "default": null
                            }
                        ]
                    }
                }
            ],
            "default": null
        },
        {
            "name": "key_metadata",
            "type": [
                "null",
                "bytes"
            ],
            "default": null
        }
    ]
})EOF";

}  // namespace iceberg
