import sys
import os
import argparse
from pathlib import Path
import zipfile
import shutil
from joblib import Parallel, delayed
import tqdm

from logging import basicConfig, getLogger, INFO
basicConfig(level=INFO, format='%(asctime)s %(levelname)s :%(message)s')
logger = getLogger(__name__)


def zip_directory(input_path, output_filename, compresslevel):
    with zipfile.ZipFile(output_filename,
                         "w",
                         zipfile.ZIP_DEFLATED,
                         compresslevel=compresslevel,
                         allowZip64=True) as zf:
        for filename in input_path.glob('**/*'):
            if filename.is_dir():
                continue
            else:
                zf.write(filename, filename.relative_to(input_path))
    return 0


def zip_file(input_path, output_filename, compresslevel):
    with zipfile.ZipFile(output_filename,
                         "w",
                         zipfile.ZIP_DEFLATED,
                         compresslevel=compresslevel,
                         allowZip64=True) as zf:
        zf.write(input_path, input_path.name)
    return 0


def zip_entry(input_path, output_filename, compresslevel):
    input_path = Path(input_path)
    if input_path.is_dir():
        return zip_directory(input_path, output_filename, compresslevel)
    else:
        return zip_file(input_path, output_filename, compresslevel)


def main():
    parser = argparse.ArgumentParser(
        description='Zip all entries in the input directory.')
    parser.add_argument('input', help="Input directory", metavar='<input>')
    parser.add_argument('output', help="Output directory", metavar='<output>')

    parser.add_argument('-j',
                        '--jobs',
                        help="# of concurrent jobs: default %(default)s",
                        metavar='<n>',
                        type=int,
                        default=-1)
    parser.add_argument(
        '-d',
        '--depth',
        help=
        "Zip entries in the input directory with the specified depth: default %(default)s",
        metavar='<n>',
        type=int,
        default=0)

    parser.add_argument('--cl',
                        help="Compress level: default %(default)s",
                        metavar='<n>',
                        type=int,
                        default=1)

    parser.add_argument('--delete',
                        help="Delete original entries",
                        action='store_true')

    args = parser.parse_args()

    indir = Path(args.input)
    outdir = Path(args.output)
    n_jobs = args.jobs if args.jobs != 0 else -1
    if not indir.exists():
        print('Error: input directory doesnt exist.')
        return 1
    if not indir.is_dir():
        print('Error: specified input is not a directory.')
        return 1

    glob_pattern = os.sep.join(['*'] * args.depth + ['*'])
    entries = list(indir.glob(glob_pattern))
    logger.info('Zip {} entries.'.format(len(entries)))

    zip_args = []
    for entry in entries:
        output_filename = outdir / entry.relative_to(indir).with_suffix('.zip')
        output_filename.parent.mkdir(exist_ok=True, parents=True)
        zip_args.append((entry, output_filename, args.cl))

    Parallel(n_jobs=n_jobs)(delayed(zip_entry)(*arg)
                            for arg in tqdm.tqdm(zip_args))
    logger.info('Done')

    if args.delete:
        logger.info('Delete {} entries'.format(len(entries)))
        for e in entries:
            shutil.rmtree(e)
        logger.info('Done')


if __name__ == "__main__":
    sys.exit(main())
