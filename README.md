# Triggerflow patch for Lithops

This repository contains all the necessary files to make the IBM-PyWren library running with the Triggerflow system. Note that this patch will modify the local `lithops` installation once applied. 

The Triggerflow patch for `lithops` is shipped with 4 different event sources: *kafka*, *Redis*, *Cloudant* and *IBM Cloud Object Store*. An event source is the place where the termination event of a functions is stored.

To run a workflow as code, the main component is the *Coordinator function*. This function contains all the actual `lithops` function invocations: `call_async()`, `map()` and `map_reduce()`. Each time The coordinator function invokes a function with one of those methods, it is immediately shut down. When the invoked function finishes its execution it sends the termination event to the configured event source, then the Triggerflow service receives this event and awakes the Coordinator function. At this point ,the coordinator function, using an event sourcing technique, recovers the state of the execution and continues invoking the other `lithops` calls, skipping those that are already done.

## Installation

The following guide provides instructions for the installation and configuration of the lithops client. 

1. Using a virtual environment is recommended (Optional).

    ```
    $ python3 -m venv tf_env
    $ source tf_env/bin/activate
    ```

2. Install Lithops library, version 2.2.5:
   
    ```
    pip install lithops==2.2.5
    ```

3. Install the Triggerflow library:
    
    ```
    git clone https://github.com/triggerflow/triggerflow
    cd triggerflow
    python setup.py install
    ```
     
4. Install The Triggerflow patch for Lithops:

    ```
    git clone https://github.com/triggerflow/lithops-patch
    cd lithops-patch
    pip install -r requeriments.txt
    python install_patch.py
    ```

## Configuration

1. Once installed, you need to create a configuration file for Lithops. [Follow this instructions](https://github.com/lithops-cloud/lithops/tree/master/config) to configure your compute and storage backends. It is recommended to place the `lithops_config.yaml` file in the root directory of your project that contains the Triggerflow scripts.

2. Edit your lithops config file and add the `triggerflow` section with your access details to your *Triggerflow* deployment. You must provide here the event source service (sink):
    ```yaml
    triggerflow:
        endpoint: http://127.0.0.1:8080
        user: admin
        password: admin
        workspace: lithops_workspace
        sink: redis
    ```

 3. Add in your lithops config file the access details to the Triggerflow event source service. In this example *redis*:
     ```yaml
     redis:
        host: 127.0.0.1
        port: 6379
        password: G6pSd9mQzeR5Dzuw2JIJjAVZWK6v
        db: 0
        stream: lithops-test-stream
        name: lithops-test
     ```

4. If you configured the Triggerflow service with Kafka event source, you must add the access details in the lithops config file, for example:
    ```yaml
    kafka:
        broker_list: [127.0.0.1:9092]
        auth_mode: None
    ```
    

## Usage

1. Create a Triggerflow workspace:
    ```python
    with open('pywren_config.yaml', 'r') as config_file:
        tf_config = yaml.safe_load(config_file)

    tf = Triggerflow(endpoint=tf_config['triggerflow']['endpoint'],
                     user=tf_config['triggerflow']['user'],
                     password=tf_config['triggerflow']['password'])

    redis_event_source = RedisEventSource(**tf_config['redis'])

    tf.create_workspace(workspace=tf_config['triggerflow']['workspace'],
                        event_source=redis_event_source)
    ```

2. Create the coordinator function, which contains all the Lithops calls, and define your functions:
    ```python
    def my_function(x):
        time.sleep(x)
        return x + 3
    
    def main(args): # Coordinator function
        fexec = lithops.FunctionExecutor(**args, log_level='INFO')
        res = 0
        fexec.call_async(my_function, int(res))
        res = fexec.get_result()
        fexec.call_async(my_function, int(res))
        res = fexec.get_result()
    ```

 3. Deploy and run the coordinator function:
     ```python
     tf_exec = TriggerflowExecutor()
     tf_exec.run(main, name='triggerflow_lithops_test')
     ```

Find complete examples in [examples/](examples/)
