outfile="Result_$1.txt"
echo "Yarn cluster manager" > $outfile
echo "Yarn cluster manager"
time spark-submit --master yarn Q$1.py 2> /dev/null 1>> $outfile
echo "Local[*]" >> $outfile
echo "Local[*]"
time spark-submit --master local[*] Q$1.py 2> /dev/null 1>> $outfile
echo "Local[6]" >> $outfile
echo "Local[6]"
time spark-submit --master local[6] Q$1.py 2> /dev/null 1>> $outfile
echo "Q$1 Finished!"
