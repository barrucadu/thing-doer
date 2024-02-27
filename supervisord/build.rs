fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("proto/authpb_auth.proto")?;
    tonic_build::compile_protos("proto/etcdserverpb_rpc.proto")?;
    tonic_build::compile_protos("proto/mvccpb_kv.proto")?;
    Ok(())
}
