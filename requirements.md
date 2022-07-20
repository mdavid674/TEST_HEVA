# OS & software required
* Ubuntu 20.04
* Java 11.0.15
* Spark Hadoop 3.3.0
* Python 3.10.4
* Anaconda 1.7.2
* VSCode (notebook extension) 1.68.1 or Jupyter Notebook

# Installation

## ANACONDA
Go to [this link](https://www.rosehosting.com/blog/how-to-install-anaconda-on-ubuntu-20-04/) to download and install anaconda.

Create the python environment
```conda env create -f test_heva.yml```

Enable Environment
```conda activate test_heva```

```pip install nbconvert[webpdf]```

conda install -c conda-forge pyppeteer


## JAVA
```sudo apt-get install openjdk11-jre```

```sudo apt-get install openjdk11-jdk```

Check that java is installed correctly
```java --version```

## SPARK HADOOP
Go to [this link](https://www.apache.org/dyn/closer.lua/spark/spark-3.3.0/spark-3.3.0-bin-hadoop3.tgz) to download hadoop-spark

Download spark-3.3.0-bin-hadoop3.tgz

```cd Downloads```

```tar xvzf spark-3.3.0-bin-hadoop3.tgz```

```sudo mv spark-3.3.0-bin-hadoop3 /usr/local/spark```

```gedit ~/.baschrc```

Copy the following lines & replace <YOUR_USER_NAME>

```export SPARK_HOME=/usr/local/spark```

```export PATH=$PATH:$SPARK_HOME/bin```

```export PYSPARK_PYTHON=/home/<YOUR_USER_NAME>/anaconda3/envs/test_heva/bin/python```

```export PYSPARK_DRIVER_PYTHON=/home/<YOUR_USER_NAME>/anaconda3/envs/test_heva/bin/python```

Save & close

```source ~/.baschrc```


## ASSISTANCE
If pyspark is not installed correctly try in the test_heva environment:
```conda install -c conda-forge pyspark=3.3.0```

***If you have an installation problem on windows OS, install Ubuntu 20.04 OS ;)***

For other problem, contact me: mdavid674@gmail.com


### DELETE

sudo apt remove default-jdk default-jre
sudo rm -r /usr/local/spark
conda remove -n test_heva --all