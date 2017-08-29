import subprocess
import pandas as pd
import glob

from datetime import datetime, timedelta

# ----------------------------------------------------------------------------
# Return a single plot without right and top axes
def fig_setup():
    fig = plt.figure(figsize=(13,7))
    ax = fig.add_subplot(111)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.get_xaxis().tick_bottom()
    ax.get_yaxis().tick_left()

    return fig, ax

# ----------------------------------------------------------------------------
# Extracts RP 0.42 Tx
def extract_rp_tx(sid):


    # Load csv file with timestamps
    profile = pd.read_csv('./%s/%s/%s.prof' % (sid.split('/')[0],
                                               sid.split('/')[1],
                                               sid.split('/')[2]),
        header=None,
        names=['tstamp','sid','uid','state','event','msg'],
        usecols=['tstamp','sid','uid','state'])

    # Keep only unit profiles
    profile = profile.dropna(subset=['uid'])
    profile = profile[profile.uid.str.contains('unit')]

    # Elminate redundant 'Done' state
    profile.loc[profile.state == 'Done'] = profile.loc[
        (profile.sid.str.contains('OutputFileTransfer')) &
        (profile.state == 'Done')]
    profile = profile.dropna()

    # Purge useless info from session ID
    profile['sid'] = profile['sid'].apply(lambda x: x.split(':')[1])
    profile = profile.reset_index(drop=True)

    # Keep only execution-related states
    txs = profile[(profile.state == 'Executing') |
                  (profile.state == 'StagingOutput') |
                  (profile.state == 'AgentStagingOutputPending')].copy()

    # Profiles are a mess in 0.42 :(
    # - Duplicates of state StagingOutput
    # - spare presence of state AgentStagingOutputPending
    for uid in txs.uid.tolist():
        txs[txs.uid == uid]
        if len(txs[
            (txs.uid == uid) &
            (txs.state == 'StagingOutput')]['state'].tolist()) >= 2:
            txs = txs.drop(txs.index[(txs.uid == uid) &
                                     (txs.state == 'StagingOutput') &
                                     (txs.sid.str.contains('Thread'))])
        if 'AgentStagingOutputPending' in txs[txs.uid == uid].state.tolist() and \
           'StagingOutput' in txs[txs.uid == uid].state.tolist():
            txs = txs.drop(txs.index[(txs.uid == uid) &
                (txs.state == 'AgentStagingOutputPending')])

    # We are done with sid, drop it
    txs = txs.drop('sid', axis=1)

    # Calculate $T_x$
    txs.tstamp = pd.to_numeric(txs.tstamp, errors='coerce')
    txs = txs.pivot(index='uid', columns='state', values='tstamp')
    txs['Tx'] = txs['StagingOutput']-txs['Executing']

    return txs

# ----------------------------------------------------------------------------
# Extracts NAMD Tx from its STDOUT file
def extract_namd_tx(sid, pname):

    stdouts = glob.glob('%s-%s-units-folder/unit.*/STDOUT' % (sid, pname))
    df = pd.DataFrame(columns=['stage','NAMD Duration'])
    df.index.name = 'uid'

    exec_prof = open('./%s/%s/execution_profile_%s.csv' % (
        sid.split('/')[0], sid.split('/')[1], sid.split('/')[2]),'r')
    read_exec_lines = exec_prof.readlines()

    for line in read_exec_lines[2:]:

        if int(line.split(',')[1].strip().split('_')[1].strip()) in [4,5,6]:

            uid = line.split(',')[0].strip()
            stage = line.split(',')[1].strip()
            out = '%s-%s-units-folder/%s/STDOUT'%(sid, pname, uid)

            f = open(out,'r')
            last_line = f.readlines()[-1:][0]

            namd_dur = float(last_line.split('~')[1].strip().split(',')[0].strip()[:-1])

            df.loc[uid] = [stage, float(namd_dur/8)]

    return df.sort_index()


# ----------------------------------------------------------------------------
# Extracts RP Tr
def extract_rp_tr(sid, pname):
    profile = '%s/%s/bootstrap_1.prof' % (sid,pname)
    duration = subprocess.check_output("grep 'ACTIVE,QED' %s | cut -f1 -d," % profile, shell=True).rstrip()
    return float(duration)


# ----------------------------------------------------------------------------
# Calculates the time overlpas among elements of the same type
def get_Toverlap(d, start_state, stop_state):
    '''
    Helper function to create the list of lists from which to calculate the
    overlap of the elements of a DataFrame between the two boundaries passed as
     arguments.
    '''

    overlap = 0
    ranges = []

    for obj, states in d.iteritems():
        #print states
        ranges.append([states[start_state], states[stop_state]])

    for crange in collapse_ranges(ranges):
        overlap += crange[1] - crange[0]

    return overlap

def collapse_ranges(ranges):
    """
    given be a set of ranges (as a set of pairs of floats [start, end] with
    'start <= end'. This algorithm will then collapse that set into the
    smallest possible set of ranges which cover the same, but not more nor
    less, of the domain (floats).

    We first sort the ranges by their starting point. We then start with the
    range with the smallest starting point [start_1, end_1], and compare to the
    next following range [start_2, end_2], where we now know that start_1 <=
    start_2. We have now two cases:

    a) when start_2 <= end_1, then the ranges overlap, and we collapse them
    into range_1: range_1 = [start_1, max[end_1, end_2]

    b) when start_2 > end_2, then ranges don't overlap. Importantly, none of
    the other later ranges can ever overlap range_1. So we move range_1 to
    the set of final ranges, and restart the algorithm with range_2 being
    the smallest one.

    Termination condition is if only one range is left -- it is also moved to
    the list of final ranges then, and that list is returned.
    """

    final = []

    # sort ranges into a copy list
    _ranges = sorted (ranges, key=lambda x: x[0])

    START = 0
    END = 1

    base = _ranges[0] # smallest range

    for _range in _ranges[1:]:

        if _range[START] <= base[END]:
            # ranges overlap -- extend the base
            base[END] = max(base[END], _range[END])

        else:

            # ranges don't overlap -- move base to final, and current _range
            # becomes the new base
            final.append(base)
            base = _range

    # termination: push last base to final
    final.append(base)

    return final


# ----------------------------------------------------------------------------
# Extracts TTX
def extract_rp_ttx(df_cu):
    '''
    Convert dataframe into a dictionary - for each uid (compute unit id), we
    get the timestamps for 'Executing' and 'AgentStagningOutputPending' states

    The structure of the dictionary is as below:
    super_dict = {
        'unit.00000': {
            'Executing': timestamp1,
            'AgentStagingOutputPending': timestamp2
        }
    }
    '''

    # Rename columns to match requirements of extract_exec_time()
    df_cu.columns = ['Executing', 'AgentStagingOutputPending']

    # Create uid column to match requirements of extract_exec_time()
    df_cu['uid'] = df.index
    df_cu.reset_index

    super_dict = dict()
    for row in df_cu.iterrows():
        row=row[1]
        uid = row['uid']
        start_probe = float(row['Executing'])
        end_probe = float(row['AgentStagingOutputPending'])

        if uid not in super_dict:
            super_dict[uid] = dict()

        if 'Executing' not in super_dict[uid]:
            super_dict[uid]['Executing'] = start_probe

        if 'AgentStagingOutputPending' not in super_dict[uid]:
            super_dict[uid]['AgentStagingOutputPending'] = end_probe


    # Use the magic function to get the total time spent between 'Executing'
    # and 'AgentStagingOutputPending'
    return get_Toverlap(super_dict, 'Executing', 'AgentStagingOutputPending')


# ---------------------------------------------------------------------------
# Extract EnTK overhead as a function of the overlapping time spent sending
# tasks to RP
def extract_entk_overhead(df_pat):
    '''
    Convert dataframe into a dictionary - for each task (we get a unique name
    by using the stage and the pipeline number in combination), we get the
    timestamps for 'start_time', 'wait_time', 'res_time' and 'done_time' events

    The structure of the dictionary is as below:
    super_dict = {
        'stage1-pipeline1': {
            'start_time': timestamp1,
            'wait_time': timestamp2,
            'res_time': timestamp3,
            'done_time': timestamp4
        }
    }
    '''

    def totimestamp(dt, epoch=datetime(1970,1,1)):
        td = dt - epoch
        #return td.total_seconds()
        return (td.microseconds + (td.seconds + td.days * 86400) * 10**6)

    super_dict = dict()
    for row in df_pat.iterrows():
        row=row[1]
        stage = row['stage']
        pipeline = row['pipeline']
        probe = row['probe']
        #timestamp = time.mktime(time.strptime(row['timestamp'],
        #   "%Y-%m-%d %H:%M:%S.%f"))
        #print datetime.strptime(row['timestamp'], "%Y-%m-%d %H:%M:%S.%f")
        precise_epoch = totimestamp(datetime.strptime(row['timestamp'],
            "%Y-%m-%d %H:%M:%S.%f"))
        # totimestamp(datetime.fromtimestamp(timestamp))
        #print timestamp, precise_epoch
        if '%s-%s'%(stage, pipeline) not in super_dict:
            super_dict['%s-%s'%(stage, pipeline)] = dict()

        if probe not in super_dict['%s-%s'%(stage, pipeline)]:
            super_dict['%s-%s'%(stage, pipeline)][probe] = precise_epoch

    return (get_Toverlap(super_dict, 'start_time', 'wait_time') +
        get_Toverlap(super_dict, 'res_time', 'done_time')) / 10**6.0

# ---------------------------------------------------------------------------
# ...
def extract_ttx(df):

    # Rename columns to match requirements of extract_rp_ttx()
    df.columns = ['Executing', 'AgentStagingOutputPending']

    # Create uid column to match requirements of extract_rp_ttx()
    df['uid'] = df.index
    df.reset_index

    # Get TTX from Tx of all the units
    ttx = extract_rp_ttx(df)

    # Return TTX
    return ttx
