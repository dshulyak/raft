Key/Value app using raft replication
===

Lineriazable reads and writes, with a caveat that operations are not
uniquely identified. Therefore same logical operation can be executed twice
under certain conditions.

How to run?
---

#### Prepare environment

There is a helm chart that can be used for automated deployment on k8s.
For local setup install [minikube](https://github.com/kubernetes/minikube).

Optionally install tools for viewing metrics and logs:

```bash
helm repo add grafana https://grafana.github.io/helm-charts

helm install monitoring grafana/loki-stack --set grafana.enabled=true,prometheus.enabled=true,prometheus.alertmanager.persistentVolume.enabled=false,prometheus.server.persistentVolume.enabled=false

kubectl expose service monitoring-grafana --type=NodePort --target-port=3000 --name=grafana-ext
```

Both prometheus and loke data sources are already configured.

#### Build an image

```bash
make image
```

If using minikube there is an option to build an image using minikube docker instance,
it will be available immediatly for all future deployments:

```bash
# before running make image
eval $(minikube -p minikube docker-env)
```

#### Deploy app

Application cluster must be aware of the ip addresses that will be externally visible,
as all requrests are served by the leader - non-leader replicas will redirect requests.

Also, each replica will be exposed separately using nodePort by default. Checkout [values file](./helm/values.yaml).

```bash
DOMAIN=$(minikube ip) make install
```

#### Chaos experiments

Install [chaos-mesh](https://chaos-mesh.org/docs/get_started/get_started_on_minikube/).
Several examples are available in `./chaos` directory.

It also needs to install several new k8s object extensions, so installation is a bit
more complicated then helm install.

```bash
curl -sSL https://mirrors.chaos-mesh.org/v1.1.1/install.sh | bash
```

#### Interacting with an app

```bash
# update
curl -X POST http://0.0.0.0:30001/write/ddd -d "dsdadassdadsa"

# read
curl http://0.0.0.0:30001/get/ddd

# curl can be used with -L to redirect to a current leader
# if leader is unknown application will block until it is elected
# unless replica is partitioned it won't block for long
# but better to set an appropriate timeout
curl -L -m 1 http://0.0.0.0:30001/get/ddd
```
