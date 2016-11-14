import os
import json
import subprocess
#from subprocess import Popen
#from subprocess import PIPE
import multiprocessing.dummy
#from multiprocessing.dummy import Pool as ThreadPool
from operator import itemgetter

class Mush():
     '''
     Individual instance of a mush run
     '''

     # TODO: set up return code for non-existant hosts, display properly
     # =-=-==-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     def __init__(self):
          self.dryrun=False
          self.path="/usr/bin:/bin:/opt/quest/bin:/bb/bin:/ts/bin"
          self.timeout=10
          self.resultset=set()
          self.poolsize=20
          self.symv=None
          self.command=None
          self.bbhosts = self.bbhostgrab()
          self.quiet=False
          self.bubbles=False
          self.startthread="O"
          self.stopthread="."
          self.monochrome=False
          self.raw=False
          return

     # =-=-==-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     def bbhostgrab(self):
          '''
          build and return bbhosts object, incorporate alias names
          '''
          # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
          # These could be useful, but for mush purposes we want them all treated
          # as equal.  Leaving this here for potential future use.
          #bbmachines = json.load(open('/bb/data/bbmachines.json')) # host entries with metadata named
          #bbcontacts = json.load(open('/bb/data/bbcontacts.json')) # maps uuid<->login
          #bbgroups = json.load(open('/bb/data/bbgroups.json')) # group descriptions, names
          #bbclusters = json.load(open('/bb/data/bbclusters.json')) # child cluster descriptions, names
          # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
          if os.path.exists('/bb/bin/bbcpu.lst') and os.path.exists('/bb/bin/bbcpu.alias'):
               bbhosts = [ x.split() for x in open('/bb/bin/bbcpu.lst').readlines() ]
               bbalias = [ x.split() for x in open('/bb/bin/bbcpu.alias').readlines() ]
               nodelist = [x[1] for x in bbhosts]
               bbindex = dict(zip(nodelist, bbhosts))
               for alias in bbalias: bbindex[alias[-1]].insert(-1, alias[0])
               bbhosts = [ tuple(x) for x in bbindex.values()]
          else:
               raise Exception('ERROR!  Unable to locate bbcpu.lst\n        %s unable to run on this host with current options\n' % sys.argv[0])
          return bbhosts

     # =-=-==-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     def symv_apply(self):
          '''
          applies symv expression string in self.symv and reates a result set
          of hostnames that match the expression.
          resultset is returned and self.resultset is defined.
          '''
          for tag in self.symv.lower().split():
               newset = set()
               prefix = tag[0]
               if prefix == '^': mode = 'intersection'
               elif prefix == '~' or prefix == '-': mode = 'difference'
               else: mode = 'union'
               if not mode == 'union': tag = tag[1:]
               if tag == "localhost" : tag = os.uname()[1]
               for entry in self.bbhosts:
                    if tag in entry:
                         newset.add(entry[0])
                         #self.resultset.add(entry[0])
               self.resultset = getattr(self.resultset, mode)(newset)
          return self.resultset

     # =-=-==-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     def runcommand (self, host):
          '''
          creates an ssh subprocess to host, runs self.command there
          bubble output is generated if self.bubbles is true
          returns a tuple containing (hostname, stdout, stderr, exit code)
          '''
          if self.bubbles: print(self.startthread),
          if not self.dryrun:
               timeout = "-o ConnectTimeout=" + str(self.timeout)
               nox11 = "-o ForwardX11=no"
               gssapi = "-o PreferredAuthentications=gssapi-keyex,gssapi-with-mic,publickey"
               remote_command = "PATH=%s %s" % (self.path, self.command)

               ssh = subprocess.Popen(["ssh", "%s" % host, timeout, nox11, gssapi, remote_command], shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
               #ssh = self.subprocess.Popen(["ssh", "%s" % host, timeout, nox11, gssapi, remote_command], shell=False, stdout=self.subprocess.PIPE, stderr=self.subprocess.PIPE)

               (result,errors) = ssh.communicate()
               return_value = ssh.wait()
               result = result.strip()
               errors = errors.strip()
          else:
               result = ''
               errors = ''
               return_value = 0
          if self.bubbles: print(self.stopthread),

          if result == '':
               result = host
          else:
               result = host + '\n' + ''.join(result)
          if self.raw:
               return {'stdout': result,'stderr': errors, 'rc': return_value}
          else:
               return (result,errors,return_value)
          #return (host,result,errors,return_value)

     # =-=-==-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     def go (self, symv=None, command=None):
          '''
          Execute mush run, symv and command must have been previously
          defined or passed in here: mush.go('symv', 'command')
          returns a list of tuples (host, stdout, stderr, exitcode)
          '''
          if symv:
               self.symv = symv
          elif not self.symv:
               raise Exception("No SYMV specified, you must specify by setting the symv property or by passing it as part of calling the go(symv, command) method")
          if command:
               self.command = command
               if "--symv" in command:
                    raise Exception("Cannot run commands with --symv options on multiple machines")
          elif not self.command:
               raise Exception("No command specified, you must specify by setting command property or passing it as part of calling the go(symv, command) method")
          if not type(self.symv) is str or not type(self.command) is str:
               raise Exception("Both symv and command are expected to be <type 'str'>\n\tsymv=%s\n\tcommand=%s" % (type(self.symv), type(self.command)))
          if not self.resultset: self.symv_apply()
          if len(self.resultset) == 0:
               raise Exception("Error: SYMV expression '%s' matched 0 hosts" % self.symv)
          if self.raw:
               self.monochrome = True
               self.quiet = True
               self.bubbles = False
          if not self.monochrome:
               colorlightblue = "\033[01;36m{0}\033[00m"
               self.startthread = colorlightblue.format(self.startthread)
               self.stopthread = colorlightblue.format(self.stopthread)
          if self.bubbles: print
          # -------------------------------------------------------------
          threadpool = multiprocessing.dummy.Pool(self.poolsize)
          results = threadpool.map(self.runcommand, self.resultset)
          # -------------------------------------------------------------
          if self.bubbles: print('')
          if self.raw:
               _results = list(results)
               results = {}
               for result in _results:
                    hostname = result['stdout'].split('\n')[0]
                    results[hostname] = result
          else:
               results = sorted(results,key=itemgetter(0))
          return results

     # =-=-==-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     def output_results (self, results, monochrome=False):
          if self.raw:
               print(json.dumps(results))
               return
          sep = "-" * 50
          sep = "-" * 70
          sep2 = "= " + ("-" * 48)
          slashline = '/' * 50
          slashline = '/' * 70
          chevron = '>>>'
          if not monochrome:
               colorred = "\033[31m{0}\033[00m"
               colorbrightred = "\033[01;31m{0}\033[00m"
               colorgreen = "\033[01;32m{0}\033[00m"
               coloryellow = "\033[01;33m{0}\033[00m"
               slashline = colorgreen.format(slashline)
               chevron = colorbrightred.format(chevron)
          connectionerrors = []
          # host by host output
          print
          print(sep)
          print(slashline)
          for result in sorted(results, key=itemgetter(0)):
               printexit=False
               print(sep)
               #  result = (stdout, stderr, retcode)
               if result[2] == 0:
                    print(result[0])
                    if printexit: print('exit %s' % result[2])
               else:
                    print(result[0])
                    if not monochrome and result[1]:
                         print(colorred.format(result[1]).strip())
                         if printexit: print('exit %s' % colorbrightred.format(result[2]))
                    elif result[1]:
                         print(result[1].strip())
                         if printexit: print('exit %s' % result[2])
               if result[2] == 255 or result[2] == '-15': # 255 bad connection, -15 sigKILL
                    connectionerrors.append(result)
          print(sep)
          print(slashline)
          # connection error output (SSH)
          if not len(connectionerrors) == 0:
               print('=' * 70)
               print('%s ssh errors:\n%s ' % ((chevron + ' ') * 7, chevron))
               connectionerrorhosts = [entry[0] for entry in connectionerrors]
               print('%s %s' % (chevron, ' '.join(connectionerrorhosts)))
               print(chevron)
               for result in sorted(connectionerrors, key=itemgetter(0)):
                    thiserror =  result[1].strip()
                    thisexit = 'exit %s' % result[2]
                    if not monochrome:
                         thiserror = coloryellow.format(thiserror)
                         thisexit = colorbrightred.format(thisexit)
                    print('%s %s' % (chevron, sep[4:]))
                    print('%s %s' % (chevron,  result[0]))
                    print('%s %s' % (chevron, thiserror))
                    print('%s %s' % (chevron, thisexit))
               print('%s %s' % (chevron, sep[4:]))
               print('%s\n%s' % (chevron, (chevron + ' ') * 7))
               print('=' * 70)
          print(sep)
          if len(connectionerrors) > 1: s = "s"
          else: s = ""
          print(' %s hosts processed. %s connection error%s' % (
               len(results), len(connectionerrors), s))
          print
          return

     # =-=-==-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     def as_dict (self):
          '''
          returns a dictionary of values representing state for this mush instance
          '''
          currentconfig={     'timeout': self.timeout,
                              'path': self.path,
                              'quiet': self.quiet,
                              'bubbles': self.bubbles,
                              'dryrun': self.dryrun,
                              #'bbhosts': self.bbhosts,
                              'command': self.command,
                              'symv': self.symv,
                              'hostlist': self.resultset         }
          return currentconfig

'''
no main block until we impliment some other features
(potentially not needed by something importing an instance of mush

* vasvalidate method and a way to specify keys only
* colored output
* output display (this currently just returns the list of tuples with results)
* add an exec_timeout option (or maybe a way to do it atomatically? like a function
  of how many db's are passed to nukedb for instance)
* --true and --false functionality (may deprecate this but it can be useful)
'''

#if __name__ == "__main__":
