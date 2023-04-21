import json
import argparse

######################################################################
# Confluent Automatic Rebalancer doesn't take account into the region. 
# when rebalancing, the preferred leaders are placed into all
# replicas in all racks. This may not be the desirable behavior for
# some use cases. 
# This tool takes the input of a pre-generated  proposed partition assignments
# and move the preferred brokers in the replica list (excluding MRC observer)
# into the top of the replica lists. For non-preferred brokers, appending
# them into the list
# For example:
# Original list: [5, 2, 4, 8, 1, 6], observers = [1,6]
# Prefer broker list: [1, 2, 4]
# The result list: [2, 4, 5, 8, 1, 6]
#######################################################################

def adjust_partition(data, prefer):
    for element in data['partitions']:
        print ("original: " + str(element['replicas']))
        replica_list = element['replicas'][0:(len(element['replicas'])-len(element['observers']))]
        observers = element['observers']
        local_replica = []
        remote_replica = []
        for i in range(len(replica_list)):
            list = local_replica if replica_list[i] in prefer else remote_replica
            list.append(replica_list[i])
        final = local_replica + remote_replica + observers
        element['replicas'] = final
        print ("result: " + str(element['replicas']))
        print ("===========")

    return data

def main():
    
    p = argparse.ArgumentParser()
    p.add_argument('-pr', nargs="+", type=int, dest='prefer', required=True, help='a list of prefer broker ids as integer (example: 1 2 3 4)')
    p.add_argument('-o', '--output', type=argparse.FileType('w'), dest='output', help="The output file you choose")
    p.add_argument('-pt', '--indent', dest='indent', help="To print pretty json, enter an indent")
    p.add_argument('-i', '--input', type=argparse.FileType('r'), dest='input', required=True, help='The input json file')


    args = p.parse_args()

    prefer = list(args.prefer)
    output_file = args.output
    input_file = args.input

    # print(prefer)

    data = adjust_partition(json.load(input_file), prefer)

    print (data) if output_file is None else json.dump(data, output_file, indent=int(args.indent)) if args.indent is not None else json.dump(data, output_file)

if __name__ == "__main__":
    main()
