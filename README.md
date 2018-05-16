# Nokdemo installation, configuration and user guide

## Installation

### Requirements

#### OS requirements

* To install nokdemo environment, you need a maintained version of CentOS 7/Oracle Linux 7. Archived versions aren’t supported or tested. The centos-extras repository must be enabled. This repository is enabled by default, but if you have disabled it, you need to re-enable it.
* Set the time zone to UTC

    `timedatectl set-timezone UTC`

#### Nokdemo Requirements

In order to install the Nokdemo environemnt you must have installed the following components:

* python
* python pip
* python virtualenv
* git

##### Python install

1. Open a terminal and add the repository to your yum install.

```bash
sudo yum install -y https://centos7.iuscommunity.org/ius-release.rpm
```

2. Update yum to finishing adding the repository.

```bash
sudo yum update
```

3. Download and install Python.

This will not only install Python – it will also install pip to help you with installing add-ons.

```bash
sudo yum install -y python36u python36u-libs python36u-devel python36u-pip
```

##### Python pip install

1. Add the EPEL Repository

Pip is part of Extra Packages for Enterprise Linux (EPEL), which is a community repository of non-standard packages for the RHEL distribution. First, we’ll install the EPEL repository.

```bash
sudo yum install epel-release
```

2. The Installation

As a matter of best practice we’ll update our packages:

```bash
yum -y update
```

Then let’s install python-pip and any required packages:

```bash
yum -y install python-pip
```

##### Python virtualenv install

In order to install virtualenv , we are going to call in pip for help. We will install it as a globally available package for the Python interpreter to run.
The simplest method to install is using pip to search, download and install. This might not provide you the latest stable version.

```bash
pip install virtualenv
```

##### Git Install

```bash
sudo yum install git
```

## Configuration

1. Clone project from GitHub

```bash
https://github.com/elefedefe/nokdemo.git
cd nokdemo
```

2. Create a virtual environment for python

```bash
virtualenv venv
```

3. Activate virtual environment

```bash
source venv/bin/activate
```

4. Install your app as a python package

```bash
python setup.py install
```

5. Install requirements

```bash
pip install -r nokdemo/requirements.txt
```

6. Run the application

```bash
python nokdemo/app.py <ST2_URL> <API_KEY_VALUE>
```

## User guide

1. Open your browser and enter the URL : `http://<NOKDEMO_URL>:5000`
2. Click "**Browse file**" or "**Escolher ficheiro**"
3. Browse for `source.csv` file location, select the file and click **Open**
4. Click **Upload**
5. Get the CGR commands and enter those commands on the indicated MSS console.
6. For each circuit group enter get the NET and SPC values and enter those values in the respective fields
7. Click on **Submit** button
8. If everything is working correctly you should see a message informing you that an issue is being created on Jira, and the respective summary and description fields.
9. Open your Jira and confirm if an issue was created on the 'NOKDEMO' (ND) project
