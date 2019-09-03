Mongodb replication tool, tested against MongoDB 3.14.10.

## How it works

Oplog records of source mongodb instance would be sliced, compressed and saved in a third mongodb instance using GridFS functionality(or optionally, in any local file directory somehow asscessible by target mongodb). Target mongodb instance then reads oplog chunks from the intermediary and replicates data.

The rationale is that it divides the whole replication process to data transfer layer and data restore layer. Network issues bewteen source/target mongo instances would only affect data transfer, since an oplog chunk read would either succeed or fail, and if it fails, the replication would not hang in an unrecoverable state(just restart the oplog_dump/oplog_replay script).

## Usage

### Dump oplogs from source

```bash
export MONGOSYNC_CONF=your_config_file_path
mongo-sync --dump
```

### Replay oplogs for target

```bash
export MONGOSYNC_CONF=your_config_file_path
mongo-sync --replay
```

### Configuration

See `mongo_sync/config_example.yaml`

Env variable `MONGOSYNC_CONF` has to be set as the config file absolute path.