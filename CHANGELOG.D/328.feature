Allow to build and push docker images to the remote image registries.

The requirement for allowing Kaniko (the underlying tool) to push such images - it should be authenticated.

The registry authention data structure should be in the following form:
`{"auths": {"<registry URI>": {"auth": "<base64 encoded '<username>:<password>' string"}}}`
(tested with the `https://index.docker.io/v1/` - public DockerHub registry)

`neuro-extras config build-registy-auth` command might become handy in this case.

To attach the target registy AUTH data into the builder job, one might save it as the platform secret and mount into the builder job.
Mounting of the secret could be done either as the ENV variable with the name preffixed by `NE_REGISTRY_AUTH`, or as the secret file.
In the later case, the ENV variable should also be added with the mentioned above preffix and pointing to the corresponding file.
