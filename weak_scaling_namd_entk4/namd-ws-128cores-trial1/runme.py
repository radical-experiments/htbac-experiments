import os, glob
import pandas as pd

if __name__ == '__main__':


    df = pd.DataFrame(columns=['BW duration','RP duration'])

    data_loc = os.path.dirname(os.path.realpath(__file__))

    exec_prof = open('%s/execution_profile_rp.session.two.jdakka.017398.0008.csv'%data_loc,'r')

    read_exec_lines = exec_prof.readlines()

    for line in read_exec_lines[2:]:

        if int(line.split(',')[1].strip().split('_')[1].strip()) in [3,4,5]:

            uid = line.split(',')[0].strip()

            stdout = open('%s/rp.session.two.jdakka.017399.0000-pilot.0000/%s/STDOUT'%(data_loc,uid),'r')

            last_line = stdout.readlines()[-1:][0]

            bw_dur = float(last_line.split('~')[1].strip().split(',')[0].strip()[:-1])
            rp_dur = float(line.split(',')[10].strip()) - float(line.split(',')[9].strip())

            df.loc[uid] = [bw_dur, rp_dur]


    print df

    ax = df.plot(kind='line')
