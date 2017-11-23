from radical.ensemblemd.kernel_plugins.kernel_base import KernelBase

# ------------------------------------------------------------------------------
#
_KERNEL_INFO = {
    "name":         "untar",                  # Mandatory
    "description":  "Expands the given tar archive",
    "arguments":   {"--inputfile=":
                        {
                        "mandatory": True,
                        "description": "The tar archive"
                        },
                    },
    "machine_configs":
    {
        "*": {
            "environment"   : None,
            "pre_exec"      : None,
            "executable"    : "tar",
            "uses_mpi"      : False
        }
    }
}

# ------------------------------------------------------------------------------
#
class UntarKernel(KernelBase):

    def __init__(self):

        super(UntarKernel, self).__init__(_KERNEL_INFO)
     	"""Le constructor."""

    # --------------------------------------------------------------------------
    #
    @staticmethod
    def get_name():
        return _KERNEL_INFO["name"]

    def _bind_to_resource(self, resource_key):
        """(PRIVATE) Implements parent class method.
        """
        if resource_key not in _KERNEL_INFO["machine_configs"]:
            if "*" in _KERNEL_INFO["machine_configs"]:
                # Fall-back to generic resource key
                resource_key = "*"
            else:
                raise NoKernelConfigurationError(kernel_name=_KERNEL_INFO["name"], resource_key=resource_key)

        cfg = _KERNEL_INFO["machine_configs"][resource_key]

        executable = "/bin/bash"
        arguments  = ['-l', '-c', 'tar zxvf {input1} -C ./'.format(input1 = self.get_arg("--inputfile="))]

        self._executable  = executable
        self._arguments   = arguments
        self._environment = cfg["environment"]
        self._uses_mpi    = cfg["uses_mpi"]
        self._pre_exec    = None
