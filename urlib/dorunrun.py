import typing
from   typing import *

def dorunrun(command:Union[str, list],
    timeout:int=None,
    verbose:bool=False,
    quiet:bool=False,
    return_exit_code:bool=False,
    ) -> Union[bool, int]:
    """
    A wrapper around (almost) all the complexities of running child 
        processes.
    command -- a string, or a list of strings, that constitute the
        commonsense definition of the command to be attemped. 
    timeout -- generally, we don't
    verbose -- do we want some narrative to stderr?
    quiet -- overrides verbose, shell, etc. 
    return_exit_code -- return the actual exit code rather than
        implicitly converting to boolean True for 0.

    returns -- True if the child process returns a zero, or the code.
    """

    if verbose: tombstone(f"{command=}")

    if isinstance(command, (list, tuple)):
        command = [str(_) for _ in command]
        shell = False

    elif isinstance(command, str):
        shell = True

    else:
        raise Exception(f"Bad argument type to dorunrun: {command}")

    r = None
    try:
        result = subprocess.run(command, 
            timeout=timeout, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE, 
            shell=shell)

        r = result.returncode

        # Always show errors even if verbose is False.
        if not r:
            verbose and tombstone("Child process terminated without error.")
        elif r < 0:
            tombstone(f"Child process terminated by signal {-r}")
        else:
            verbose and tombstone(f"Child process returned an error: {r}")

    except subprocess.TimeoutExpired as e:
        tombstone(f"Process exceeded time limit at {e.timeout} seconds.")    

    except Exception as e:
        tombstone(f"Unexpected error: {str(e)}")
    
    return result.returncode if return_exit_code else (r == 0)



