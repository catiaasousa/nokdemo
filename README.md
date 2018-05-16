# NOC Automation DEMO (nokdemo) Installation and Configuration

## Requirements

### OS requirements

* To install nokdemo environment, you need a maintained version of CentOS 7/Oracle Linux 7. Archived versions aren’t supported or tested. The centos-extras repository must be enabled. This repository is enabled by default, but if you have disabled it, you need to re-enable it.
* Set the time zone to UTC

    `timedatectl set-timezone UTC`

### NOKDEMO Requirements

In order to install the Nokdemo environemnt you must have installed the following components:

* Docker
* Docker Compose
* git
* curl
* wget

#### Docker Installation

##### SET UP THE REPOSITORY

1. Install required packages. `yum-utils` provides the `yum-config-manager` utility, and `device-mapper-persistent-data` and `lvm2` are required by the `devicemapper` storage driver.

```bash
sudo yum install -y yum-utils device-mapper-persistent-data lvm2
```

2. Use the following command to set up the stable repository. You always need the stable repository, even if you want to install builds from the edge or test repositories as well.

```bash
sudo yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo
```

##### INSTALL DOCKER CE

1. Install the latest version of Docker CE, or go to the next step to install a specific version:

```bash
sudo yum install docker-ce
```

2. Start Docker.

```bash
sudo systemctl start docker
```

3. Verify that docker is installed correctly by running the hello-world image.

```bash
sudo docker run hello-world
```

##### POST INSTALATION STEPS

###### Manage Docker as a non-root user

1. Create a docker group

```bash
sudo groupadd docker
```

2. Add your user to the docker group.

```bash
sudo usermod -aG docker $USER
```

3. Log out and log back in so that your group membership is re-evaluated.

4. Verify that you can run `docker` commands without `sudo`

```bash
docker run hello-world
```

#### Install docker-compose

1. Run this command to download the latest version of Docker Compose:

```bash
sudo curl -L https://github.com/docker/compose/releases/download/1.21.0/docker-compose-$(uname -s)-$(uname -m) -o /usr/local/bin/docker-compose
```

2. Apply executable permissions to the binary:

```bash
sudo chmod +x /usr/local/bin/docker-compose
```

#### Install git

```bash
sudo yum install git
```

#### Install wget and curl

```bash
sudo yum install -y wget curl
```

# Install nokdemo environment

## Install StackStorm

1. Clone StackStorm configuration files from GitHub, create environment files and run docker container with all configurations

```bash
git clone https://github.com/StackStorm/st2-docker.git
cd st2-docker
make env
docker-compose up -d
```

## Install Jira

1. Create a new directory for jira docker files

```bash
mkdir jira-docker
cd jira-docker
```

2. Get docker compose configuration file and run the container

```bash
curl -O https://raw.githubusercontent.com/blacklabelops/jira/master/docker-compose.yml
docker-compose up -d
```

### Configure Jira for first use

1. Point your browser to the Jira Server address `http://<JIRA_URL>`
2. Wait for first time jira configuration
3. Set up Jira Properties
    * **Application Title** - Name of this installation (Ex: st2jira)
    * **Mode** - `Private`
    * **Base URL** - The base URL for this installation of JIRA.
    * Click **Next**
4. Specify your license key
    1. If you haven't a license key already, **generate** one.
    * Select **Product** = `JIRA Service Desk`
    * **License Type** = `JIRA Service Desk (Server)`
    * **Organization** = `AltranPT`
    * **Your instance is** = `up and running`
    * **Server ID** = `keep default`
    2. Click **Generate License**
    3. Confirm you IP address
    4. Click **Next**
    5. Wait while your license id being configured
5. Set up administrator account
6. Skip email notifications setup
    Wait while the final step of the JIRA installation is being performed...
7. Choose the language you want to use in JIRA.
8. Skip avatar creation
9. Create a **sample project** and choose **Project Management**
    1. Enter **Project Name** = `NOK DEMO`
    2. **Key** = `ND`
    3. Click **Submit**

## Install nokdemo web application

1. Pull nokdemo docker image

```bash
docker pull elefedefe/nokdemo
```

2. Run nokdemo container passing the stackstorm server `<ST2_ADDRESS>` as an environment variable

    * To get the stackstorm server address you must `docker container ls` and check for the name of the stackstorm container
    * In this example stackstorm container has the name `st2-docker_stackstorm_1`

```bash
docker run --rm -d -p 5000:5000 --name nokdemo elefedefe/nokdemo <ST2_ADDRESS> <API_KEY_VALUE>
```

## Create a dataflow network for docker containers

1. Create a docker network

```bash
docker network create nokdemonet
```

# Nokdemo environment setup

## Conventions and usefull commands

* **ST2_URL** : address of your StackStorm instance outside container
* **ST2_ADDRESS** : address of your StackStorm instance inside your container
* **JIRA_URL** : address of your Jira instance outside your container
* **JIRA_ADDRESS** : address of your JIra instance inside your container
* **NOKDEMO_URL** : address of your Nokdemo instance outside your container
* **NOKDEMO_ADDRESS** : address of your Nokdemo instance inside your container

### How to get internal address

An internal address is one that is only known inside your dataflow network and is the same as your container name.
To get the name of your host inside the dataflow network  execute:

```bash
docker ps --format "{{.Names}}"
```

### Get Server Address (outside address)

1. Run the following command on your servers terminal:

```bash
ip addr show
```

## Connect Nokdemo, StackStorm and Jira containers to docker network

1. Connect containers to `nokdemonet`**

```bash
docker network connect nokdemonet nokdemo
docker network connect nokdemonet jira-docker_jira_1
docker network connect nokdemonet st2-docker_stackstorm_1
```

2. Check that containers were successfuly assigned to dataflow network

```bash
docker network inspect nokdemonet
```

And look at the `Containers` section of the command output.

```json
 "Containers": {
            "2eae4ae7544bfa15658242c9894ac970befecedbf89ad70d80b49da333c62402": {
                "Name": "nokdemo",
                "EndpointID": "a3b5558a8ee0358bafa79d74e411ba6fcf8bf7fc0db1c77d81e561f62cc73332",
                "MacAddress": "02:42:ac:13:00:02",
                "IPv4Address": "172.19.0.2/16",
                "IPv6Address": ""
            },
            "4650778fb8bfa22cabb704214d67ee7306f5f6008c10b416dd48f7fbf8b713b5": {
                "Name": "nokdemo_jira_1",
                "EndpointID": "b3528dc5b8a742327fbc3d689fd49dacffd4d2a3ff07049234474859be308bd7",
                "MacAddress": "02:42:ac:13:00:04",
                "IPv4Address": "172.19.0.4/16",
                "IPv6Address": ""
            },
            "b326ca4355ee6a70b94770fb3c7c33639e575e5eeb0f9c0797b57f1a55c9a8e9": {
                "Name": "stackstorm",
                "EndpointID": "7e9623c9bbcffdaab2942a39d3e962953b3f254d02b1c62092ff459985892ecc",
                "MacAddress": "02:42:ac:13:00:03",
                "IPv4Address": "172.19.0.3/16",
                "IPv6Address": ""
            }
        }
```

2. Search for your ethernet interface (usualy called `ens33`) and look at its properties:

```bash
ens33: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500 qdisc pfifo_fast state UP qlen 1000
    link/ether 00:0c:29:44:e2:fb brd ff:ff:ff:ff:ff:ff
    inet 192.168.230.131/24 brd 192.168.230.255 scope global dynamic ens33
```

3. In this example `192.168.230.131` is your <**SERVER_ADDRESS**>

## Configure Jira Package in stackstorm

### Install Jira Package

#### Using CLI

1. Enter StackStorm container (execute on `~/st2-docker/`)

```bash
docker-compose exec stackstorm bash
```

2. Execute inside stackstorm container

```bash
$ st2 pack install jira
```

#### Using Web UI

1. Point your browser to your StackStorm URL <ST2_URL> `https://<ST2_URL>`
2. Enter your credentials
    1. Get your credentials from `st2-docker/conf/stackstorm.env`
3. Browse to **Packs** menu
4. Search for the available packages and select **Jira**
5. Click **Install**

### Configure tokens

1. Generate RSA public/private key pair
    Change directory to `/opt/stackstorm/configs` 
    ```bash
    # This will create a 2048 length RSA private key
    $openssl genrsa -out mykey.pem 2048
    ```
    ```bash
    # Now, create the public key associated with that private key
    openssl rsa -in mykey.pem -pubout
    ```
2. Generate a consumer key. You can use python uuid.uuid4() to do this, for example:

```python
$ python
Python 2.7.10 (default, Jul 30 2016, 19:40:32)
[GCC 4.2.1 Compatible Apple LLVM 8.0.0 (clang-800.0.34)] on darwin
Type "help", "copyright", "credits" or "license" for more information.
>>> import uuid
>>> print uuid.uuid4()
210660f1-ca8a-40d5-a6ee-295ccbf3074d
>>>
```

3. Configure JIRA for external access:
    1. Go to **AppLinks** section of your JIRA - `https://<JIRA_URL>/plugins/servlet/applinks/listApplicationLinks`
    2. Create a Generic Application with some fake URL
        * **Address** = `http://stackstorm.local`
        * Click **Create new link**
        * If a message is displayed stating: 
        > No response was received from the URL you entered - it may not be valid. Please fix the URL below, if needed, and click Continue.

        Click **Continue**
    3. Edit Application Settings
        * Set **Application Name** = `stackstorm`
        * **Application Type** = `Generic Application`
        * Check **Create incoming link**
        * Click **Continue**
    4. Plug in the consumer key and RSA public key you generated.
        * **Consumer key**: `Enter the consumer key generated in step 2.`
        * **Consumer Name**: `stackstorm`
        * **Public key**: `Enter the public key generated in step 1.`
        * Click **Continue**

4. Get access token using this [script](https://github.com/lakshmi-kannan/jira-oauth-access-token-generator/blob/master/generate_access_token.py). You may need to install additional libraries to run that script, and you will need to edit the script to use your file locations. Check the [README](https://github.com/lakshmi-kannan/jira-oauth-access-token-generator/blob/master/README.md) file for more information. The access token is printed at the **end** of running that script. Save these keys somewhere safe.

    1. Change directory to `/opt/stackstorm/configs`

    2. Activate jira virtualenv

    ```bash
    source /opt/stackstorm/virtualenvs/jira/bin/activate
    ```

    3. Download the token generation script

    ```bash
    curl -O https://raw.githubusercontent.com/lakshmi-kannan/jira-oauth-access-token-generator/master/generate_access_token.py
    ```

    4. Edit `generate_access_token.py` script to use your file locations

    ```bash
    CONSUMER_KEY = u'432208d5-7246-4a68-aa8a-a2c974f4d9ca' # change to your consumer key
    RSA_KEY = read('/opt/stackstorm/configs/mykey.pem') # change to location of your privete key
    JIRA_SERVER = 'http://<JIRA_ADDRESS>:8080' # change to your path to Jira Server (get <JIRA_ADDRESS> using `docker network inspect nokdemonet`, and get the address from containers name. Don't forget that the tcp port Jira Server is exposing on your dataflow network is 8080)
    ```
    5. Run `python generate_access_token.py`
    6. You should see something like the following:

    ```bash
    STEP 1: GET REQUEST TOKEN
    oauth_token=Yk3qAmqz0oR4DT1gmOsHN9I2xgrJBGYe
    oauth_token_secret=LHB87HvgSwjAl7uKGY8jWsBEkSoDCYZ5


    STEP2: AUTHORIZATION
    Visit to the following URL to provide authorization:
    http://<JIRA_ADDRESS>:8080/plugins/servlet/oauth/authorize?oauth_token=Yk3qAmqz0oR4DT1gmOsHN9I2xgrJBGYe


    Press any key to continue...
    ```

    7. Visit the URL but change it with your **<JIRA_URL>** Address (**Remember**: inside the container Jira is listening on port 8080 and there you should use the name of the container as address `jira-docker_jira_1`, BUT outside you should use your JIRA_URL in port 80)
    8. Click **Allow**
    9. Return to your StackStorm container console and **Press any key to continue**
    10. After pressing a key you should get the access tokens, that you should save these keys somewhere safe...

    ```bash
    STEP2: GET ACCESS TOKEN
    oauth_token=ZANKcavL03kaH5SjBnUXYD5sWEtqJ8Dv
    oauth_token_secret=LHB87HvgSwjAl7uKGY8jWsBEkSoDCYZ5
    ```

5. Plug in the access token and access secret into the sensor or action. You are good to make JIRA calls. Note: OAuth token expires. You'll have to repeat the process based on the expiry date.
    1. Return to your browser, point to StackStorm URL and setup Jira package with the access tokens you've just received in **STEP2: GET ACCESS TOKEN**
        1. Point your browser to StackStorm address https://<ST2_URL>
        2. Enter your credentials
            Get your credentials from `st2-docker/conf/stackstorm.env`
        3. Browse to **Packs** menu
        4. Search for the installed packages and select **Jira**
        5. On the right side of your screen enter:
            * Your **Consumer key**
            * **OAuth secret**
            * **OAuth token**
            * **Project**: `ND`
            * **rsa_cert_file**: `/opt/stackstorm/configs/mykey.pem`
            * **URL**: `http://<JIRA_ADDRESS>:8080`
            * Click **Save**

### Test the connection between StackStorm and Jira

#### Using CLI

1. List actions for Jira pack

```bash
st2 action list -p jira
```

2. Run the action to create an issue in Jira

```bash
st2 run jira.create_issue summary="Test Summary" type="Task" project="ND" description="Test Description"
```

#### Using Web UI

1. In the StackStorm Web UI select the **Actions** menu
2. Browse to **Jira** pack
3. Select **Create Issue** action
4. At the right side of Screen enter the following arguments
    * **summary** : `Test Summary`
    * **type** : `Task`
    * **project** : `ND`
    * **descrpition** : `Test description`
5. Click **Run**

## Setup a webhook for nokdemo web application

In order to have our nokdemo web application to send data into stackstorm a webhook must be set as a passive sensor on StackStorm.
Webhooks allow you to integrate external systems with StackStorm using HTTP webhooks. Unlike sensors which use a “pull” approach, webhooks use a “push” approach. They push triggers directly to the StackStorm API using HTTP POST requests.
Here we are going to create a site specific pack for our nokdemo environment.

### Create a stackstorm pack

A “pack” is the unit of deployment for integrations and automations that extend StackStorm. Typically a pack is organized along service or product boundaries e.g. AWS, Docker, Sensu etc. A pack can contain Actions, Workflows, Rules, Sensors, and Aliases.
On our project we are going to create a new pack called `nokdemo`, to pack our actions, rules and actions, including our Webhook, that we'll define as a rule definition.

1. Create the pack folder structure and related files under `~/st2-docker/packs.dev` outside your container. Let’s keep the metadata files such as `pack.yaml`, `config.schema.yaml`, and `requirements.txt` empty for now:

```bash
# Use the name of the pack for the folder name.
mkdir nokdemo
cd nokdemo
mkdir actions
mkdir rules
mkdir sensors
mkdir aliases
mkdir policies
touch pack.yaml
touch requirements.txt
```

**Note**: All folders are optional. It is safe to skip a folder or keep it empty. Only create the `config.schema.yaml` file if it is required. An empty schema file is not valid.

2. Create the pack definition file, pack.yaml:

```yaml
---
ref: nokdemo
name: nokdemo
description: Simple pack containing rules for webhook with nokdemo web application.
keywords:
    - nokdemo
    - webhook
version: 0.1.0
author: Altran PT
email: altran@altran.com
```

3. In order to have the pack configuration registered on StackStorm database, you need to run `st2ctl reload` command. Within the container, run the following:

```bash
st2ctl reload --register-all

... output trimmed ...
```

### Register a WebHook

All requests to the `/api/v1/webhooks` endpoints need to be authenticated in the same way as other API requests. So the firs step is to create an API key.

1. Create an API key
    To create an API key:

    ```bash
    st2 apikey create -k -m '{"used_by": "nokdemo web application"}'
    <API_KEY_VALUE>
    ```

    Save your `API_KEY_VALUE` on a safe place
2. Registering a Webhook
    You can register a webhook in StackStorm by specifying the `core.st2.webhook` trigger inside a rule definition.

    You can register a new webhook either using CLI or the Web UI:

    #### Using CLI

    1. Inside the your stackstorm docker container, change directory to `/opt/stackstorm/packs.dev/nokdemo/rules/` and create a new file called `webrule.yaml`
    2. Edit this file and paste the following code:

    ```yaml
    ---
    name: webrule
    pack: "nokdemo"
    description: Flask web app to jira
    enabled: true

    trigger:
        type: "core.st2.webhook"
        parameters:
            url: "nokdemo"

    criteria: {}

    action:
    ref: "jira.create_issue"
    parameters:
        summary: '{{trigger.body.summary}}'
        project: '{{trigger.body.project}}'
        type: '{{trigger.body.type}}'
        description: '{{trigger.body.description}}'
    ```
    The `url:` parameter above is added as a suffix to /api/v1/webhooks/ to create the URL to POST data to. So once you have created the rule above, you can use this webhook by POST-ing data to your StackStorm server at `https://<ST2_URL>/api/v1/webhooks/nokdemo`.

    3. As usual reload your new configuration using the command: `st2ctl reload --register-all`

    #### Using Web UI

    1. On your browser go to **Rules** menu
    2. At the botton of your screen click on the "**+**" sign to create a new rule
    3. Enter the following on the the fields bellow:
        * **name** : `webrule`
        * **pack** : `nokdemo`
        * **description** : `Flask web app to Jira`
        * **enable** : checked
        * **trigger**
            * **name** : `core.st2.webhook`
            * **url** : `nokdemo`
        * **action** :
            * **name** : `jira.create_issue`
            * **summary** : `{{trigger.body.summary}}`
            * **project** : `{{trigger.body.project}}`
            * **type** : `{{trigger.body.type}}`
            * **description** : `{{trigger.body.description}}`
    4. Click **CREATE**

3. Test your Webhook
    1. First list your registered webhooks

    ```bash
    st2 webhook list
    ```

    1. Use `curl` to check your connection from your nokdemo container

    ```bash
    curl -X POST https://<ST2_ADDRESS>/api/v1/webhooks/nokdemo -H "St2-Api-Key: <API_KEY_VALUE>" -H "Content-Type: application/json" --data '{"summary": "Test summary", "type": "Task", "description": "Test description", "project": "ND" }'
    ```

# Test your nokdemo application

1. Open your browser and enter the URL : `http://<NOKDEMO_URL>:5000`
2. Click "**Browse file**" or "**Escolher ficheiro**"
3. Browse for `source.csv` file location, select the file and click **Open**
4. Click **Upload**
5. Get the CGR commands and enter those commands on the indicated MSS console.
6. For each circuit group enter get the NET and SPC values and enter those values in the respective fields
7. Click on **Submit** button
8. If everything is working correctly you should see a message informing you that an issue is being created on Jira, and the respective summary and description fields.
9. Open your Jira and confirm if an issue was created on the 'NOKDEMO' (ND) project

# Troubleshooting

1. StackStorm packs unavailable in web ui

    **Problem**
    There are no available packs in the Web UI

    **Cause**
    Your StackStorm docker container is not able to access Internet, because is using wrong DNS servers

    **Solution**
    On your host machine insert your DNS server address on /etc/docker/daemon.json

    ```json
    {
	    "dns": ["dns1_ip", "dns2_ip"]
    }
    ```

2. Cannot edit files inside StackSTorm container

    **Problem**
    While trying to edit my scripts in INSERT mode with `vi` I cannot insert new characters

    **Cause**
    It's a bug

    **Solution**
    Install `vim` or `nano` instead

    ```bash
    apt-get install vim nano
    ```

3. Cannot load .CSV in Nokdemo web app (Error connecting to server [Errno -3] Temporary failure in name resolution)

    **Problem**
    While trying to upload the `source.csv` file a message appears stating:

    ````
    requests.exceptions.ConnectionError: HTTPSConnectionPool(host='stackstorm', port=443): Max retries exceeded with url: /api/v1/webhooks/nokdemo (Caused by NewConnectionError('<urllib3.connection.VerifiedHTTPSConnection object at 0x7fbf0490e588>: Failed to establish a new connection: [Errno -3] Temporary failure in name resolution',)) 
    ````

    **Cause**
    You are not passing the correct StackStorm address to the docker environement variable. Remember inside your docker container network the address name should bem the name of your container

    **Solution**
    1. Check the container names

    ```bash
    docker ps --format "{{.Names}}"
    ```

    Get the name for your StackStorm instance that you should change where you see <ST2_ADDRESS> in the commands bellow

    2. Restart your nokdemo container with the right stackstorm address

    ```bash
    docker stop nokdemo
    docker rm nokdemo
    docker run --rm -d -p 5000:5000 --name nokdemo elefedefe/nokdemo <ST2_ADDRESS> <API_KEY_VALUE>
    ```
