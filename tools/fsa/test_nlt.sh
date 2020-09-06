
for f in test*.txt; do
  for ((nl=1; nl < 4; nl++)); do
    env LD_LIBRARY_PATH=../../build/Linux-x86_64-g++-6.3-bmi2-1/lib \
        dbg/nlt_build.exe -n $nl -o $f.nlt $f
    if ! diff <(LC_ALL=C sort $f) <(dbg/dfa_text.exe $f.nlt); then
      echo Fail on text file $f 1>&2
      exit
    fi
  done
done
