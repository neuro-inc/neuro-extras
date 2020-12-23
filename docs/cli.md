# CLI reference

## neuro-extras

Auxiliary scripts and recipes for automating routine tasks.

**Usage:**

```bash
neuro-extras [OPTIONS] COMMAND [ARGS]...
```

**Options:**

| Name | Description |
| :--- | :--- |
| _--version_ | Show the version and exit. |
| _--help_ | Show this message and exit. |

**Command Groups:**

| Usage | Description |
| :--- | :--- |
| [_neuro-extras config_](cli.md#neuro-extras-config) | Configuration operations. |
| [_neuro-extras data_](cli.md#neuro-extras-data) | Data transfer operations. |
| [_neuro-extras image_](cli.md#neuro-extras-image) | Job container image operations. |
| [_neuro-extras k8s_](cli.md#neuro-extras-k8s) | Cluster Kubernetes operations. |
| [_neuro-extras seldon_](cli.md#neuro-extras-seldon) | Seldon deployment operations. |

**Commands:**

| Usage | Description |
| :--- | :--- |
| [_neuro-extras download_](cli.md#neuro-extras-download) | Download neuro project files from storage. |
| [_neuro-extras init-aliases_](cli.md#neuro-extras-init-aliases) | Create neuro CLI aliases for neuro-extras functionality. |
| [_neuro-extras upload_](cli.md#neuro-extras-upload) | Upload neuro project files to storage. |

### neuro-extras config

Configuration operations.

**Usage:**

```bash
neuro-extras config [OPTIONS] COMMAND [ARGS]...
```

**Options:**

| Name | Description |
| :--- | :--- |
| _--help_ | Show this message and exit. |

**Commands:**

| Usage | Description |
| :--- | :--- |
| [_neuro-extras config save-docker-json_](cli.md#neuro-extras-config-save-docker-json) | Generate JSON configuration file for accessing cluster registry. |

#### neuro-extras config save-docker-json

Generate JSON configuration file for accessing cluster registry.

**Usage:**

```bash
neuro-extras config save-docker-json [OPTIONS] PATH
```

**Options:**

| Name | Description |
| :--- | :--- |
| _--help_ | Show this message and exit. |

### neuro-extras data

Data transfer operations.

**Usage:**

```bash
neuro-extras data [OPTIONS] COMMAND [ARGS]...
```

**Options:**

| Name | Description |
| :--- | :--- |
| _--help_ | Show this message and exit. |

**Commands:**

| Usage | Description |
| :--- | :--- |
| [_neuro-extras data cp_](cli.md#neuro-extras-data-cp) | Copy data between external object storage and cluster. |
| [_neuro-extras data transfer_](cli.md#neuro-extras-data-transfer) | Copy data between storages on different clusters. |

#### neuro-extras data cp

Copy data between external object storage and cluster. Supported external object storage systems: ['AWS', 'GCS', 'AZURE', 'HTTP', 'HTTPS']

**Usage:**

```bash
neuro-extras data cp [OPTIONS] SOURCE DESTINATION
```

**Options:**

| Name | Description |
| :--- | :--- |
| _-x, --extract_ | Perform extraction of SOURCE into the DESTINATION directory. The archive type is derived from the file name. Supported types: .tar.gz, .tgz, .tar.bz2, .tbz2, .tbz, .tar, .gz, .zip. |
| _-c, --compress_ | Perform compression of SOURCE into the DESTINATION file. The archive type is derived from the file name. Supported types: .tar.gz, .tgz, .tar.bz2, .tbz2, .tbz, .tar, .gz, .zip. |
| _-v, --volume MOUNT_ | Mounts directory from vault into container. Use multiple options to mount more than one volume. |
| _-e, --env VAR=VAL_ | Set environment variable in container. Use multiple options to define more than one variable. |
| _-t, --use-temp-dir_ | Download and extract / compress data \(if needed\)  inside the temporal directory. Afterwards move resulted file\(s\) into the DESTINATION. NOTE: use it if 'storage:' is involved and extraction or compression is performed to speedup the process. |
| _--help_ | Show this message and exit. |

#### neuro-extras data transfer

Copy data between storages on different clusters.

**Usage:**

```bash
neuro-extras data transfer [OPTIONS] SOURCE DESTINATION
```

**Options:**

| Name | Description |
| :--- | :--- |
| _--help_ | Show this message and exit. |

### neuro-extras image

Job container image operations.

**Usage:**

```bash
neuro-extras image [OPTIONS] COMMAND [ARGS]...
```

**Options:**

| Name | Description |
| :--- | :--- |
| _--help_ | Show this message and exit. |

**Commands:**

| Usage | Description |
| :--- | :--- |
| [_neuro-extras image build_](cli.md#neuro-extras-image-build) | Build Job container image remotely on cluster using Kaniko. |
| [_neuro-extras image transfer_](cli.md#neuro-extras-image-transfer) | Copy images between clusters. |

#### neuro-extras image build

Build Job container image remotely on cluster using Kaniko.

**Usage:**

```bash
neuro-extras image build [OPTIONS] PATH IMAGE_URI
```

**Options:**

| Name | Description |
| :--- | :--- |
| _-f, --file TEXT_ |  |
| _--build-arg TEXT_ |  |
| _-v, --volume MOUNT_ | Mounts directory from vault into container. Use multiple options to mount more than one volume. Use --volume=ALL to mount all accessible storage directories. |
| _-e, --env VAR=VAL_ | Set environment variable in container Use multiple options to define more than one variable |
| _-s, --preset PRESET_ | Predefined resource configuration \(to see available values, run `neuro config show`\) |
| _-F, --force-overwrite_ | Build even if the destination image already exists. |
| _--cache / --no-cache_ | Use kaniko cache while building image  \[default: True\] |
| _--verbose BOOLEAN_ |  |
| _--help_ | Show this message and exit. |

#### neuro-extras image transfer

Copy images between clusters.

**Usage:**

```bash
neuro-extras image transfer [OPTIONS] SOURCE DESTINATION
```

**Options:**

| Name | Description |
| :--- | :--- |
| _-F, --force-overwrite_ | Transfer even if the destination image already exists. |
| _--help_ | Show this message and exit. |

### neuro-extras k8s

Cluster Kubernetes operations.

**Usage:**

```bash
neuro-extras k8s [OPTIONS] COMMAND [ARGS]...
```

**Options:**

| Name | Description |
| :--- | :--- |
| _--help_ | Show this message and exit. |

**Commands:**

| Usage | Description |
| :--- | :--- |
| [_neuro-extras k8s generate-registry-secret_](cli.md#neuro-extras-k8s-generate-registry-secret) |  |
| [_neuro-extras k8s generate-secret_](cli.md#neuro-extras-k8s-generate-secret) |  |

#### neuro-extras k8s generate-registry-secret

**Usage:**

```bash
neuro-extras k8s generate-registry-secret [OPTIONS]
```

**Options:**

| Name | Description |
| :--- | :--- |
| _--name TEXT_ |  |
| _--help_ | Show this message and exit. |

#### neuro-extras k8s generate-secret

**Usage:**

```bash
neuro-extras k8s generate-secret [OPTIONS]
```

**Options:**

| Name | Description |
| :--- | :--- |
| _--name TEXT_ |  |
| _--help_ | Show this message and exit. |

### neuro-extras seldon

Seldon deployment operations.

**Usage:**

```bash
neuro-extras seldon [OPTIONS] COMMAND [ARGS]...
```

**Options:**

| Name | Description |
| :--- | :--- |
| _--help_ | Show this message and exit. |

**Commands:**

| Usage | Description |
| :--- | :--- |
| [_neuro-extras seldon generate-deployment_](cli.md#neuro-extras-seldon-generate-deployment) |  |
| [_neuro-extras seldon init-package_](cli.md#neuro-extras-seldon-init-package) |  |

#### neuro-extras seldon generate-deployment

**Usage:**

```bash
neuro-extras seldon generate-deployment [OPTIONS] MODEL_IMAGE_URI
                                               MODEL_STORAGE_URI
```

**Options:**

| Name | Description |
| :--- | :--- |
| _--name TEXT_ |  |
| _--neuro-secret TEXT_ |  |
| _--registry-secret TEXT_ |  |
| _--help_ | Show this message and exit. |

#### neuro-extras seldon init-package

**Usage:**

```bash
neuro-extras seldon init-package [OPTIONS] [PATH]
```

**Options:**

| Name | Description |
| :--- | :--- |
| _--help_ | Show this message and exit. |

### neuro-extras download

Download neuro project files from storage.

Downloads file (or files under) from storage://remote-project-dir/PATH to project-root/PATH. You can use "." for PATH to download whole project. The "remote-project-dir" is set using .neuro.toml config, as in example:

[extra] remote-project-dir = "project-dir-name"

**Usage:**

```bash
neuro-extras download [OPTIONS] PATH
```

**Options:**

| Name | Description |
| :--- | :--- |
| _--help_ | Show this message and exit. |

### neuro-extras init-aliases

Create neuro CLI aliases for neuro-extras functionality.

**Usage:**

```bash
neuro-extras init-aliases [OPTIONS]
```

**Options:**

| Name | Description |
| :--- | :--- |
| _--help_ | Show this message and exit. |

### neuro-extras upload

Upload neuro project files to storage.

Uploads file (or files under) project-root/PATH to storage://remote-project- dir/PATH. You can use "." for PATH to upload whole project. The "remote- project-dir" is set using .neuro.toml config, as in example:

[extra] remote-project-dir = "project-dir-name"

**Usage:**

```bash
neuro-extras upload [OPTIONS] PATH
```

**Options:**

| Name | Description |
| :--- | :--- |
| _--help_ | Show this message and exit. |


