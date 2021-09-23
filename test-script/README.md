## Run test configs in parallel on multiple workers

### How to run
1. Finish the learning mode and generate all the test configs on a master node(could be any worker node)
2. Create a file called `hosts` under this test-script directory and list all the node's address. Except for this master node, write `:` to indicate that this is a local node. An example of a `hosts` file with the vm1.com being the master:
    ```
    :
    ubuntu@vm2.com
    ubuntu@vm3.com
    ```
3. On this master node's test-script directory, run:  
    `bash runtest.sh`  
    If you want to pull from your own docker repo, modify the first step in the runtest.sh to add `-d ${DOCKERREPO}` to the python command.  

### Behavior  
This shell script will
1. generate all the docker pull commands and test commands needed into files.
2. Run the same docker pull commands on all nodes
3. scp configs files to all worker nodes
4. Run all tests in distributed manner on the nodes  
    e.g. if we have three jobs and 2 vms
    ```
    1. python3 sieve.py -p yugabyte-operator -c config-1.yaml
    2. python3 sieve.py -p yugabyte-operator -c config-2.yaml
    3. python3 sieve.py -p yugabyte-operator -c config-3.yaml
    ```
    GNU parallel will distribute the jobs dynamically as
    ```
    vm1:
    python3 sieve.py -p yugabyte-operator -c config-1.yaml
    python3 sieve.py -p yugabyte-operator -c config-3.yaml
    vm2:
    python3 sieve.py -p yugabyte-operator -c config-2.yaml
    ```

### Note
It assumes the sieve project home directory is under `/home/ubuntu`