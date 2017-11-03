from radical.entk import Pipeline, Stage, Task, AppManager, ResourceManager
import os
import traceback
# ------------------------------------------------------------------------------
# Set default verbosity

if os.environ.get('RADICAL_ENTK_VERBOSE') == None:
    os.environ['RADICAL_ENTK_VERBOSE'] = 'INFO'


class NamdTask(Task):
    def __init__(self, name, cores):
        super(NamdTask, self).__init__()
        self.name = name
        self.executable = ['/bin/sleep']
        self.arguments = ['0']
	self.cores = cores
        #self.mpi = mpi
	#self.cpu_reqs = {'processes': cores, 'process_type': None, 'threads_per_process': 1, 'thread_type': None}

if __name__ == '__main__':
    # Set up parameters

    cores_per_pipeline = 16
    rootdir = 'bace1_b01'
    pipelines = set()
    replicas = 64
    lambdas  = [0.0]
    workflow = ['min', 'eq1', 'eq2', 'prod']


    # Generate pipelines

    for replica in range(replicas):
        for ld in lambdas:
            p = Pipeline()

            for step in workflow:
                s, t = Stage(), NamdTask(name=step, cores=cores_per_pipeline)
               # t.arguments = ['replica_{}/lambda_{}/{}.conf'.format(replica, ld, step), '&>', 'replica_{}/lambda_{}/{}.log'.format(replica, ld, step)]
		s.add_tasks(t)
                p.add_stages(s)

            pipelines.add(p)


    # Resource and AppManager

    res_dict = {
        'resource': 'ncsa.bw_aprun',
        'walltime': 15,
        'cores': replicas * len(lambdas) * cores_per_pipeline,
        'project': 'bamm',
        'queue': 'high',
        #'cpus' : replicas * len(lambdas) * cores_per_pipeline,
	'project': 'bamm',
	'access_schema': 'gsissh'}

    # Create Resource Manager object with the above resource description
    rman = ResourceManager(res_dict)

    # FIXME this is not going to work. `rootdir` has to be copied over, but
    # only once. If `rootdir` is tarred up, then you have to untar it at then
    # other end. Where would you put that 1 untaring proccess?
    
    #rman.shared_data = [rootdir]

    # Create Application Manager
    appman = AppManager(port=32775)

    # Assign resource manager to the Application Manager
    appman.resource_manager = rman

    # Assign the workflow as a set of Pipelines to the Application Manager
    appman.assign_workflow(pipelines)

    # Run the Application Manager
    appman.run()
