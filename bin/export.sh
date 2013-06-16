__cur_dir=`readlink  /proc/$$/cwd`

__cmd="java -classpath $__cur_dir -jar $__cur_dir/bootstrap.jar"

echo "$__cmd"

$__cmd
