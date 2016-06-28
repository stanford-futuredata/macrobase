import os
import sys


def _run(cmd, save_log):
  if save_log is not None:
    cmd += ' > {}'.format(save_log)
  print 'running the following command:'
  print cmd
  exit_status = os.system(cmd)
  if exit_status != 0:
    sys.exit(exit_status)


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
  _run(macrobase_cmd, save_log)


def run_diagnostic(cmd, conf,
                  save_log=None, **kwargs):
  extra_args = ' '.join(['-D{key}={value}'.format(key=key, value=value)
                         for key, value in kwargs.items()])
  macrobase_cmd = '''java {extra_args} -Xms128m -Xmx16G \\
      -cp "src/main/resources/:target/classes:target/lib/*:target/dependency/*:target/test-classes" \\
      macrobase.diagnostic.DiagnosticMain {cmd} {conf_file}'''.format(
      cmd=cmd, conf_file=conf, extra_args=extra_args)
  _run(macrobase_cmd, save_log)
