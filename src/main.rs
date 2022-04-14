use futures::StreamExt;
use near_lake_framework::LakeConfig;
use near_lake_framework::near_indexer_primitives::types::AccountId;
use near_lake_framework::near_indexer_primitives::views::{
    StateChangeValueView, StateChangeWithCauseView,
};

#[tokio::main]
async fn main() -> Result<(), tokio::io::Error> {
    // create a NEAR Lake Framework config
    let config = LakeConfig {
        s3_endpoint: None,
        s3_bucket_name: "near-lake-data-testnet".to_string(), // AWS S3 bucket name
        s3_region_name: "eu-central-1".to_string(), // AWS S3 bucket region
        start_block_height: 87298916, // the latest block height we've got from explorer.near.org for testnet
    };

    // instantiate the NEAR Lake Framework Stream
    let mut stream = near_lake_framework::streamer(config);

    // read the stream events and pass them to a handler function with
    // concurrency 1
    /*let mut handlers = tokio_stream::wrappers::ReceiverStream::new(stream)
        .map(|streamer_message| handle_streamer_message(streamer_message))
        .buffer_unordered(1usize);

    while let Some(_handle_message) = handlers.next().await {}*/

    // Finishing the boilerplate with a busy loop to actually handle the stream
    while let Some(streamer_message) = stream.recv().await {
        println!("\r\nBlock {} {}", streamer_message.block.header.height, streamer_message.block.author);
        for shard in streamer_message.shards {

            for ff in shard.state_changes {
                println!("state_changes {:?}", serde_json::to_value(ff.value));
            }

            for ff in shard.receipt_execution_outcomes {

                println!("logs: {:#?}", ff.execution_outcome.outcome.logs);
            }

            /*for state_change in shard.state_changes {

                // We want to print the block height and
                // change type if the StateChange affects one of the accounts we are watching for
                /*if is_change_watched(&state_change, watching_list)*/ {
                    // We convert it to JSON in order to show it is possible
                    // also, it is easier to read the printed version for this tutorial
                    // but we don't encourage you to do the same in your indexer. It's up to you
                    let changes_json = serde_json::to_value(state_change)
                        .expect("Failed to serialize StateChange to JSON");
                    println!(
                        "#{}. {}",
                        streamer_message.block.header.height, changes_json["type"]
                    );
                    println!("{:#?}", changes_json);
                }
            }*/
        }

        //handle_streamer_message(streamer_message, &opts.accounts).await;
    }

    Ok(())
}

fn is_change_watched(state_change: &StateChangeWithCauseView, watching_list: &[AccountId]) -> bool {
    // get the affected account_id from state_change.value
    // ref https://docs.rs/near-primitives/0.12.0/near_primitives/views/enum.StateChangeValueView.html
    let account_id = match &state_change.value {
        StateChangeValueView::AccountUpdate { account_id, .. } => account_id,
        StateChangeValueView::AccountDeletion { account_id } => account_id,
        StateChangeValueView::AccessKeyUpdate { account_id, .. } => account_id,
        StateChangeValueView::AccessKeyDeletion { account_id, .. } => account_id,
        StateChangeValueView::DataUpdate { account_id, .. } => account_id,
        StateChangeValueView::DataDeletion { account_id, .. } => account_id,
        StateChangeValueView::ContractCodeUpdate { account_id, .. } => account_id,
        StateChangeValueView::ContractCodeDeletion { account_id, .. } => account_id,
    };

    // check the watching_list has the affected account_id from the state_change
    watching_list.contains(account_id)
}

// The handler function to take the entire `StreamerMessage`
// and print the block height and number of shards
async fn handle_streamer_message(
    streamer_message: near_lake_framework::near_indexer_primitives::StreamerMessage,
) {
    println!("fdfdfdfd {}", streamer_message.block.header.height);
    eprintln!(
        "{} / shards {}",
        streamer_message.block.header.height,
        streamer_message.shards.len()
    );
}
