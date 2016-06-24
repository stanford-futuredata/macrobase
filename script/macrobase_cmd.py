import os
import sys


def run_macrobase(cmd='pipeline', conf='conf/batch.conf', save_log=None,
                  profiler=None, **kwargs):
  extra_args = ' '.join(['-D{key}={value}'.format(key=key, value=value)
                         for key, value in kwargs.items()])
  if profiler == 'yourkit':
    extra_args += ' -agentpath:/Applications/YourKit-Java-Profiler-2016.02.app/Contents/Resources/bin/mac/libyjpagent.jnilib'  # noqa
  macrobase_cmd = '''java {extra_args} -Xms128m -Xmx16G \\
      -cp "src/main/resources/:target/classes:target/lib/*:target/dependency/*" \\
      macrobase.MacroBase {cmd} {conf_file}'''.format(
      cmd=cmd, conf_file=conf, extra_args=extra_args)
  if save_log is not None:
    macrobase_cmd += ' > {}'.format(save_log)
  print 'running the following command:'
  print macrobase_cmd
  exit_status = os.system(macrobase_cmd)
  if exit_status != 0:
    sys.exit(exit_status)
