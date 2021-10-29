# Build and install Kcat

## Fedora

> Note: If you want to use a more recent version of confluent you can find the latest version in the [versions-interoperability](https://docs.confluent.io/platform/current/installation/versions-interoperability.html
) page

Add the Confluent repo:

```bash
cat > "/etc/yum.repos.d/confluent.repo" << EOF
[Confluent.dist]
name=Confluent repository (dist)
baseurl=https://packages.confluent.io/rpm/6.2/8
gpgcheck=1
gpgkey=https://packages.confluent.io/rpm/6.2/archive.key
enabled=1
EOF
```

Now install the required build dependency:

```bash
# Update yum
yum update -y

# Install build tools 
yum group install "Development Tools" -y

# Install librdkafka and other deps
yum install -y librdkafka-devel yajl-devel avro-c-devel
```

Clone the kcat repo locally:

```bash
git clone https://github.com/edenhill/kcat.git
cd kcat
```

Checkout the version you want build from:

```bash
git checkout 1.7.0
```

Prepare the installation - make sure that this step does not result in an error!

```bash
./configure
```

Build and install:

```bash
make
sudo make install
```

Check that it works:

```bash
kcat -V
```