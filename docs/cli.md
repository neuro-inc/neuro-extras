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
| _-v, --verbose_ | Give more output. Option is additive, and can be used up to 2 times. |
| _-q, --quiet_ | Give less output. Option is additive, and can be used up to 2 times. |
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
| [_neuro-extras init-aliases_](cli.md#neuro-extras-init-aliases) | Create neuro CLI aliases for neuro-extras functionality. |

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
| [_neuro-extras config build-registy-auth_](cli.md#neuro-extras-config-build-registy-auth) | Generate docker auth for accessing remote registry. |
| [_neuro-extras config save-registry-auth_](cli.md#neuro-extras-config-save-registry-auth) | Save docker auth file for accessing platform registry. |

#### neuro-extras config build-registy-auth

Generate docker auth for accessing remote registry.

**Usage:**

```bash
neuro-extras config build-registy-auth [OPTIONS] REGISTRY_URI USERNAME
                                              PASSWORD
```

**Options:**

| Name | Description |
| :--- | :--- |
| _--help_ | Show this message and exit. |

#### neuro-extras config save-registry-auth

Save docker auth file for accessing platform registry.

**Usage:**

```bash
neuro-extras config save-registry-auth [OPTIONS] PATH
```

**Options:**

| Name | Description |
| :--- | :--- |
| _--cluster TEXT_ | Cluster name for which the auth information should be saved. Current cluster by default |
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

Copy data between external object storage and cluster. Supported external object storage systems: ['AWS', 'GCS', 'AZURE', 'HTTP', 'HTTPS']. Note: originally, Azure's blob storage scheme is 'http(s)', but we prepend 'azure+' to differenciate https vs azure

**Usage:**

```bash
neuro-extras data cp [OPTIONS] SOURCE DESTINATION
```

**Options:**

| Name | Description |
| :--- | :--- |
| _-x, --extract_ | Perform extraction of SOURCE into the DESTINATION directory. The archive type is derived from the file name. Supported types: .tar.gz, .tgz, .tar.bz2, .bz2, .tbz, .tar, .gz, .zip. |
| _-c, --compress_ | Perform compression of SOURCE into the DESTINATION file. The archive type is derived from the file name. Supported types: .tar.gz, .tgz, .tar.bz2, .bz2, .tbz, .tar, .gz, .zip. |
| _-v, --volume MOUNT_ | Mounts directory from vault into container. Use multiple options to mount more than one volume. |
| _-e, --env VAR=VAL_ | Set environment variable in container. Use multiple options to define more than one variable. |
| _-t, --use-temp-dir_ | DEPRECATED - need for temp dir is automatically detected, this flag will be removed in a future release. Download and extract / compress data \(if needed\) inside the temporary directory. Afterwards move resulted file\(s\) into the DESTINATION. NOTE: use it if 'storage:' is involved and extraction or compression is performed to speedup the process. |
| _-s, --preset PRESET\_NAME_ | Preset name used for copy. |
| _-l, --life\_span SECONDS_ | Copy job life span in seconds. |
| _--help_ | Show this message and exit. |

#### neuro-extras data transfer

Copy data between storages on different clusters.

Consider archiving dataset first for the sake of performance, if the dataset contains a lot (100k+) of small (< 100Kb each) files.

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
| [_neuro-extras image local-build_](cli.md#neuro-extras-image-local-build) | Build Job container image locally \(requires Docker daemon\). |
| [_neuro-extras image transfer_](cli.md#neuro-extras-image-transfer) | Copy images between clusters. |

#### neuro-extras image build

Build Job container image remotely on cluster using Kaniko.

**Usage:**

```bash
neuro-extras image build [OPTIONS] CONTEXT_PATH IMAGE_URI
```

**Options:**

| Name | Description |
| :--- | :--- |
| _-f, --file TEXT_ | Relative \(w.r.t. context\) path to the dockerfile. The dockerfile should be within the context directory.  \[default: Dockerfile\] |
| _--build-arg VAR=VAL_ | Build-time variables passed in ARG values, similarly to Docker. Could be used multiple times for multiple arguments. |
| _-v, --volume MOUNT_ | Mounts directory from vault into container. Use multiple options to mount more than one volume. Use --volume=ALL to mount all accessible storage directories. |
| _-e, --env VAR=VAL_ | Set environment variable in container Use multiple options to define more than one variable |
| _-s, --preset PRESET_ | Predefined resource configuration \(to see available values, run `neuro config show`\) |
| _-F, --force-overwrite_ | Overwrite if the destination image already exists. |
| _--cache / --no-cache_ | Use Kaniko cache while building image.  \[default: cache\] |
| _--verbose BOOLEAN_ | If specified, run Kaniko with 'debug' verbosity, otherwise 'info' \(default\). |
| _--build-tag VAR=VAL_ | Set tag\(s\) for image builder job. We will add tag 'kaniko-builds:{image-name}' authomatically. |
| _-p, --project PROJECT\_NAME_ | Start image builder job in other than the current project. |
| _--help_ | Show this message and exit. |

#### neuro-extras image local-build

Build Job container image locally (requires Docker daemon).

**Usage:**

```bash
neuro-extras image local-build [OPTIONS] CONTEXT_PATH IMAGE_URI
```

**Options:**

| Name | Description |
| :--- | :--- |
| _-f, --file TEXT_ | Relative \(w.r.t. context\) path to the dockerfile. The dockerfile should be within the context directory.  \[default: Dockerfile\] |
| _--build-arg VAR=VAL_ | Build-time variables passed in ARG values. Could be used multiple times for multiple arguments. |
| _-F, --force-overwrite_ | Overwrite if the destination image already exists. |
| _--verbose BOOLEAN_ | If specified, provide verbose output \(default False\). |
| _-p, --project PROJECT\_NAME_ | Start image builder job in other than the current project. |
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


