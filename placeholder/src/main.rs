use grpc_etcd::etcdserverpb::{kv_client, range_request, PutRequest, RangeRequest};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = kv_client::KvClient::connect("http://127.0.0.1:2379").await?;

    let put_request = tonic::Request::new(PutRequest {
        key: b"foo".into(),
        value: b"bar".into(),
        lease: 0,
        prev_kv: false,
        ignore_value: false,
        ignore_lease: false,
    });
    let put_response = client.put(put_request).await?;
    dbg!(put_response);

    let get_request = tonic::Request::new(RangeRequest {
        key: b"foo".into(),
        range_end: b"fop".into(),
        limit: 0,
        revision: 0,
        sort_order: range_request::SortOrder::None.into(),
        sort_target: range_request::SortTarget::Key.into(),
        serializable: false,
        keys_only: false,
        count_only: false,
        min_mod_revision: 0,
        max_mod_revision: 0,
        min_create_revision: 0,
        max_create_revision: 0,
    });
    let get_response = client.range(get_request).await?;
    dbg!(get_response);

    Ok(())
}
