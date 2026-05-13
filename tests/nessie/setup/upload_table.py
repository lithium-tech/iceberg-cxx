import pynessie

client = pynessie.init(config_dict={
                'endpoint': 'http://127.0.0.1:19120/api/v1',
                'verify': True,
                'default_branch': 'main',
            })

main_branch = client.get_reference("main")
main_hash = main_branch.hash_


namespace = pynessie.model.Namespace(id=None, elements=["default"])
namespace_key = pynessie.model.ContentKey.from_path_string('default')
put_operation = pynessie.model.Put(key=namespace_key, content=namespace)
client.commit(
    "main",
    main_hash,
    None,
    None,
    put_operation
)

table_key = pynessie.model.ContentKey.from_path_string('default.test')

iceberg_table = pynessie.model.IcebergTable(
    id=None,
    metadata_location="file:///some_snap",
    snapshot_id=1,
    schema_id=1,
    spec_id=1,
    sort_order_id=0
)

put_operation = pynessie.model.Put(key=table_key, content=iceberg_table)
client.commit(
    "main",
    main_hash,
    None,
    None,
    put_operation
)
