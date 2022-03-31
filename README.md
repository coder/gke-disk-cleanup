# `gke-disk-cleanup`

This is a small utility to aid in cleaning up unused disks in a GKE environment.

## Usage

```shell
Usage:
  gke-disk-cleanup [command]

Available Commands:
  cleanup     cleanup disks in gcloud
  help        Help about any command
  mark        mark disks for later deletion

Flags:
      --dry-run             only log the actions that would be taken (default true)
  -h, --help                help for gke-disk-cleanup
      --project-id string   google project id (default "default")
      --verbose             verbose output
      --zone string         google compute zone (default "us-east1-a")
```

`gke-disk-cleanup` operates in two phases:

### `mark` phase

In the `mark` phase, disks in the specified project and zone are marked with a label `marked-for-deletion:true` based on their last attached timestamp.
If the label `marked-for-deletion:true` is already present and the disk was attached within the specified cutoff period, the label value is updated to `marked-for-deletion:false`.
If the label `marked-for-deletion` is present with any value other than `true`, no further action will be taken.

**Note:** by default:

- Disks that have not been attached in the last 30 days will be marked. This is configurable with the `--cutoff` parameter.
- Only disks with the label `goog-gke-volume` are considered. To change this, use the `--filter` argument. See the [gcloud documentation](https://cloud.google.com/sdk/gcloud/reference/topic/filters) for more information on this topic.
- Nothing will happen unless you explicitly pass the option `--dry-run=false`.

### `cleanup` phase

In the `cleanup` phase, disks in the project and zone with the label `marked-for-deletion:true` will be snapshotted and deleted. Snapshot creation can be suppressed with the option `--do-snapshot=false`.

**Note:** by default, the `cleanup` command will do nothing unless you pass the option `--dry-run=false`.

## Getting Started

1. Ensure you have application default credentials available: `gcloud auth application-default login`
1. Ensure you have `go` installed.
1. Clone the git repository, navigate to it, and run `make build`.
1. Run `./gke-disk-cleanup --help` to see the available options.
