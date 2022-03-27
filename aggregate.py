import typer
import re
import glob
import pandas

# Used to create simple CLI
app = typer.Typer()

def getExecutionTime(filename: str, application: str):
    wallTime = ""
    try:
        with open(filename) as f:
            if application == "nwchem":
                contents = f.read()
                m = re.search(r'Total time:(.*)wall:(\s*)(\d*)s', contents)
                if not m:
                    raise Exception(f'Could not find execution time in file {filename}')
                wallTime = m.group(3) + "s" # Indicating seconds
            elif application == "icon":
                # Add logic to get execution time from file 
                pass
            elif application == "xcompact3d":
                # Add logic to get execution time from file
                pass
    except Exception as e:
        # Could do other error handling, for now just re-raise exception
        raise e
    return wallTime


def parseOneFile(filename: str, application: str):
    # Extract details from file name
    pattern = re.compile(r"stdout_np=(\d+),ppn=(\d+),nt=(\d+),mpi=(.+),job=(.+).txt")
    m = pattern.match(filename)
    if m is None:
        raise Exception(f'Invalid filename "{filename}": Must match this regex -- {pattern.pattern}')
    executionTime = getExecutionTime(filename)
    mpiLib = m.group(4)
    rowDf = pandas.DataFrame({
        "NProcs": [m.group(1)],
        "ProcsPerNode": [m.group(2)],
        "NThreads": [m.group(3)],
        "MpiLib": [m.group(4)],
        "ExecutionTime": [m.group(5)],
        "JobId": [m.group(6)]
    })

    # Insert data into CSV
    csvFileName = f'{mpiLib}.csv'
    csvFile = glob.glob(csvFileName)
    df = rowDf
    if len(csvFile) > 0:
        # Add to existing CVS
        df = pandas.read_csv(csvFileName)
        df = pandas.concat([df, rowDf])
        df.sort_values(["NProcs", "ProcsPerNode", "NThreads", "ExecutionTime"], axis=0, ascending=False, inplace=True)
    df.to_csv(csvFileName, index=False)


@app.command()
def parseDirectory(application: str, dir: str = "."):
    txtFiles = glob.glob(dir + "/*.txt")
    for f in txtFiles:
        try:
            parseOneFile(f)
        except Exception as e:
            print(f'Skipping file {f} due to exception "{str(e)}"')

if __name__ == "__main__":
    app()
