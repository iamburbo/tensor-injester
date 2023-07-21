
# Tensor Injester



## Usage

```bash
injester
--mode -m <history or standard>
--marketplace <address> defaults to Tensor address
--sigFetchSize <number> defaults to 1000
--sigWriteSize <number> defaults to 50
--part -p <part> a, b, or c
--proxy -x <proxy url> for debugging, optional

```

Outputs are stored in files named either partA.csv, partB.csv, or partC.csv. Both history and standard mode output to same file.
