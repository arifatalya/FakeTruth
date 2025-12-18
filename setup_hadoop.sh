#!/bin/bash
# HADOOP 3.4.1 PSEUDO-DISTRIBUTED SETUP
# Ubuntu + Java 17 + Spark 4.0.1

set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

HADOOP_VERSION="3.4.1"
HADOOP_URL="https://dlcdn.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz"
HADOOP_HOME="/opt/hadoop"
HADOOP_DATA="$HOME/hadoop_data"

echo -e "${BLUE}===== HADOOP PSEUDO-DISTRIBUTED SETUP =====${NC}"

# Step 1: Prerequisites
echo -e "\n${YELLOW}[1/7] Checking prerequisites...${NC}"
command -v java &>/dev/null || { echo "Java not found!"; exit 1; }
echo -e "${GREEN}✓ Java OK${NC}"

sudo apt install -y openssh-server openssh-client 2>/dev/null || true
[ ! -f ~/.ssh/id_rsa ] && ssh-keygen -t rsa -P "" -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys 2>/dev/null || true
chmod 600 ~/.ssh/authorized_keys 2>/dev/null || true
echo -e "${GREEN}✓ SSH OK${NC}"

# Step 2: Download
echo -e "\n${YELLOW}[2/7] Downloading Hadoop...${NC}"
cd ~
[ ! -f "hadoop-${HADOOP_VERSION}.tar.gz" ] && wget -q --show-progress ${HADOOP_URL}
echo -e "${GREEN}✓ Downloaded${NC}"

# Step 3: Install
echo -e "\n${YELLOW}[3/7] Installing Hadoop...${NC}"
[ ! -d "hadoop-${HADOOP_VERSION}" ] && tar -xzf hadoop-${HADOOP_VERSION}.tar.gz
sudo rm -rf ${HADOOP_HOME}
sudo mv hadoop-${HADOOP_VERSION} ${HADOOP_HOME}
sudo chown -R $USER:$USER ${HADOOP_HOME}
echo -e "${GREEN}✓ Installed at ${HADOOP_HOME}${NC}"

# Step 4: Environment
echo -e "\n${YELLOW}[4/7] Setting environment...${NC}"
cp ~/.bashrc ~/.bashrc.bak.$(date +%s)
sed -i '/# HADOOP CONFIG/,/# END HADOOP/d' ~/.bashrc

cat >> ~/.bashrc << 'ENV'

# HADOOP CONFIG
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
# END HADOOP
ENV

source ~/.bashrc
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export HADOOP_HOME=/opt/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
echo -e "${GREEN}✓ Environment set${NC}"

# Step 5: Configure
echo -e "\n${YELLOW}[5/7] Configuring Hadoop...${NC}"
CONF="${HADOOP_HOME}/etc/hadoop"

echo "export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64" >> ${CONF}/hadoop-env.sh

# core-site.xml
echo '<?xml version="1.0"?>
<configuration>
<property><name>fs.defaultFS</name><value>hdfs://localhost:9000</value></property>
</configuration>' > ${CONF}/core-site.xml

# hdfs-site.xml
echo "<?xml version=\"1.0\"?>
<configuration>
<property><name>dfs.replication</name><value>1</value></property>
<property><name>dfs.namenode.name.dir</name><value>file://${HADOOP_DATA}/namenode</value></property>
<property><name>dfs.datanode.data.dir</name><value>file://${HADOOP_DATA}/datanode</value></property>
<property><name>dfs.permissions.enabled</name><value>false</value></property>
</configuration>" > ${CONF}/hdfs-site.xml

echo -e "${GREEN}✓ Configured${NC}"

# Step 6: Format HDFS
echo -e "\n${YELLOW}[6/7] Setting up HDFS...${NC}"
mkdir -p ${HADOOP_DATA}/{namenode,datanode,tmp}

if [ ! -d "${HADOOP_DATA}/namenode/current" ]; then
    ${HADOOP_HOME}/bin/hdfs namenode -format -force
fi
echo -e "${GREEN}✓ HDFS ready${NC}"

# Step 7: Helper scripts
echo -e "\n${YELLOW}[7/7] Creating helper scripts...${NC}"

echo '#!/bin/bash
$HADOOP_HOME/sbin/start-dfs.sh
sleep 2 && jps
echo "Web UI: http://localhost:9870"' > ~/start_hdfs.sh
chmod +x ~/start_hdfs.sh

echo '#!/bin/bash
$HADOOP_HOME/sbin/stop-dfs.sh' > ~/stop_hdfs.sh
chmod +x ~/stop_hdfs.sh

echo '#!/bin/bash
echo "Creating HDFS directories..."
hdfs dfs -mkdir -p /user/$USER
hdfs dfs -mkdir -p /user/news/streaming/csv
hdfs dfs -mkdir -p /user/news/streaming/parquet
hdfs dfs -ls /user/news
echo "Done!"' > ~/test_hdfs.sh
chmod +x ~/test_hdfs.sh

echo -e "${GREEN}✓ Scripts created${NC}"

echo -e "\n${GREEN}===== SETUP COMPLETE! =====${NC}"
echo ""
echo "NEXT STEPS:"
echo "  1. source ~/.bashrc"
echo "  2. ~/start_hdfs.sh"
echo "  3. ~/test_hdfs.sh"
echo ""
echo "Web UI: http://localhost:9870"
