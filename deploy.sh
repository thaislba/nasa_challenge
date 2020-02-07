#!/bin/bash
HOME_PATH="$(dirname $(readlink -f "$0"))"
#===========================================================================================
SPARK_HOME="${HOME_PATH}/spark-2.3.4-bin-hadoop2.7" # Mudar aqui caso queira usar seu Spark!
#===========================================================================================
echo "Current path: ${HOME_PATH}"
JUL=NASA_access_log_Jul95.gz
AUG=NASA_access_log_Aug95.gz
if test -f "${HOME_PATH}/JUL/${JUL}"; then
  echo "$JUL já foi baixado"
else
  echo "Baixando $JUL..."
  mkdir -p JUL
  cd JUL
  wget https://github.com/thaislba/nasa_challenge/raw/master/NASA_access_log_Jul95.gz
  cd ..
fi
if test -f "${HOME_PATH}/AUG/${AUG}"; then
  echo "$AUG já foi baixado"
else
  echo "Baixando $AUG..."
  mkdir -p AUG
  cd AUG
  wget https://github.com/thaislba/nasa_challenge/raw/master/NASA_access_log_Aug95.gz
  cd ..
fi
sbt test && 
sbt assembly &&
${SPARK_HOME}/bin/spark-submit \
  --master "local[*]" \
  --class br.com.smtx.nasa_challenge ${HOME_PATH}/target/scala-2.11/nasa_challenge-assembly-1.0.jar \
  "file://${HOME_PATH}"
