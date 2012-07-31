A = LOAD 'names.txt' USING PigStorage();
B = A;
help
STORE A INTO 'names2.txt' USING PigStorage();
