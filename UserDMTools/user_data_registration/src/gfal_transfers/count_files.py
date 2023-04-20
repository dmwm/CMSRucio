import argparse
import glob


def _count_generator(reader):
    b = reader(1024 * 1024)
    while b:
        yield b
        b = reader(1024 * 1024)


def count_lines(filepath):
    with open(filepath, 'rb') as fp:
        c_generator = _count_generator(fp.raw.read)
        # count each \n
        count = sum(buffer.count(b'\n') for buffer in c_generator)
    return count+1


def count_non_blank_lines(filepath):
    count = 0
    with open(filepath, "r") as fp:
        for line in fp:
            if line.strip():
                count += 1
    return count


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="Line Counter")
    parser.add_argument("directoryPath")

    args = parser.parse_args()

    directoryPath = args.directoryPath

    total_lines = 0
    total_non_blank_lines = 0

    for filepath in glob.iglob(f'{directoryPath}/*'):

        total_lines = total_lines + count_lines(filepath)
        total_non_blank_lines = total_non_blank_lines + count_non_blank_lines(filepath)

    print("total_lines: ", total_lines)
    print("total_non_blank_lines: ", total_non_blank_lines)
