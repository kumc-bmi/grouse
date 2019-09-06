import pandas as pd
import os
import sys


def formate_dt_cols(col_list, exception_dt_cols, dt_col_keyword):
    output = []

    for col in col_list:
        if dt_col_keyword in col.lower() and col.lower() not in exception_dt_cols:

            formated_date_col = "%s %s" % (col, date_fmt)
            output.append(formated_date_col)
        else:

            output.append(col)
    return output


def create_cdm_ctl_files(csv_file, ora_table, sep, col_list, date_fmt,
                         exception_dt_cols, dt_col_keyword):

    formated_dt_cols = formate_dt_cols(
        col_list, exception_dt_cols, dt_col_keyword)

    cols = '\n'.join(formated_dt_cols)

    base_string = \
        '''load data infile  %s
truncate into table %s
fields terminated by '%s'
(
%s
)''' % (csv_file, ora_table, sep, cols)

    return base_string


def main(csv_folder, csv_extension, sep, exception_dt_cols, dt_col_keyword,
         date_fmt, output_folder):

    csv_files = [f for f in os.listdir(csv_folder) if csv_extension in f]

    for csv_file in csv_files:

        csv_path = os.path.join(csv_folder, csv_file)
        ora_table = csv_file.split('.')[0].lower()

        col_list = pd.read_csv(csv_path, nrows=0, sep=sep).columns.tolist()

        ctl_str = create_cdm_ctl_files(
            csv_file, ora_table, sep, col_list, date_fmt, exception_dt_cols,
            dt_col_keyword)

        output_file = "%s.ctl" % (ora_table)
        output_path = os.path.join(output_folder, output_file)
        print(output_path)
        with open(output_path, 'w') as f:
            f.write(ctl_str)


if __name__ == "__main__":

    if len(sys.argv) != 7:
        raise Exception("Incorrect number of arguments")

    [csv_folder, csv_extension, sep,
     dt_col_keyword, date_fmt, output_folder] = sys.argv[1:]

    # not sure how to pass list through argument
    exception_dt_cols = ['death_date_impute']

    main(csv_folder, csv_extension, sep,
         exception_dt_cols, dt_col_keyword, date_fmt, output_folder)

# python create_cdm_ctl.py 'C:\Users\lpatel\projects\grouse\2019 grouse\MCW\ok_to_delete-delim_export_GROUSE_KU_2019_03_04-dir' '.dsv' '|' 'date' "DATE 'YYYY-MM-DD'" 'C:\Users\lpatel\projects\repos\grouse\staging\stage_sites\scripted_cdm_ctl'
