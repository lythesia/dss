use crate::msg::{
    CommitRequest, CommitResponse, GetRequest, GetResponse, PrewriteRequest, PrewriteResponse,
    TimestampRequest, TimestampResponse,
};

labrpc::service! {
    service timestamp {
        rpc get_timestamp(TimestampRequest) returns (TimestampResponse);
    }
}

#[allow(unused_imports)]
pub use timestamp::{add_service as add_tso_service, Client as TsoClient};

labrpc::service! {
    service transaction {
        rpc get(GetRequest) returns (GetResponse);
        rpc prewrite(PrewriteRequest) returns (PrewriteResponse);
        rpc commit(CommitRequest) returns (CommitResponse);
    }
}

#[allow(unused_imports)]
pub use transaction::{add_service as add_transaction_service, Client as TxnClient};
