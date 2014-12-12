import json
import sys, getopt
import urlparse

import psycopg2

def main(argv):
    input_file = ''
    server_string = ''
    table = ''
    try:
        opts, args = getopt.getopt(argv, "hi:o:t:", ["ifile=", "oserver=","table="])
    except getopt.GetoptError:
        print 'j2p.py -i <inputfile> -o <server_string> -t table'
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print 'j2p.py -i <inputfile> -o <server_string> -t table'
            sys.exit()
        elif opt in ("-i", "--ifile"):
            input_file = arg
        elif opt in ("-o", "--oserver"):
            server_string = arg
        elif opt in ("-t", "--table"):
            table = arg

    if input_file == "" or server_string == "" or table == "":
        print 'Please supply input and output (j2p.py -i <inputfile> -o <server_string> -t table)'
        sys.exit()

    # Lets read the file and create a json obj out of it
    json_data = open(input_file)
    data = json.load(json_data)

    #lets try to figure out what keys are in the file
    keys = {}

    if isinstance(data, (list, tuple)):
        for i in data:
            for k in i.keys():
                k = k.replace("/", "_").lower()
                keys[k] = None
    else:
        for k in data.keys():
            k = k.replace("/", "_").lower()
            keys[k] = None

    #So now we have the keys now lets see if the DB has all the fields we need

    #All this is copied from https://github.com/kennethreitz/dj-database-url/blob/master/dj_database_url.py

    #Parse the URL
    url = urlparse.urlparse(server_string)

    if url.scheme != 'postgres':
        print 'Currently we only support postgres. Think about committing some code :)'
        sys.exit()

    # Remove query strings.
    path = url.path[1:]
    path = path.split('?', 2)[0]

    # Handle postgres percent-encoded paths.
    hostname = url.hostname or ''
    if '%2f' in hostname.lower():
        hostname = hostname.replace('%2f', '/').replace('%2F', '/')

    server_url = {
        'NAME': path or '',
        'USER': url.username or '',
        'PASSWORD': url.password or '',
        'HOST': hostname,
        'PORT': url.port or '',
    }

    try:
        conn = psycopg2.connect("dbname='%s' user='%s' host='%s' port='%s' password='%s'" %
                                (
                                  server_url['NAME'],
                                  server_url['USER'],
                                  server_url['HOST'],
                                  server_url['PORT'],
                                  server_url['PASSWORD']
                                ))
    except psycopg2.OperationalError:
        print "I am unable to connect to the database %s" % str(server_url)

    #Get the cursor
    cur = conn.cursor()

    #Get all the columns

    cur.execute("Select * FROM %s; " % table)

    colnames = [desc[0] for desc in cur.description]

    for i in colnames:
        keys[i.lower()] =  1

    for key in keys.keys():
        if keys[key] == None:
            #We need to create a new column
            cur.execute("ALTER TABLE %s ADD COLUMN %s text;" % (table, key))

    conn.commit()

    #Now all the Columns are there so we can add the data


    #Check if we already have it
    total = len(data)
    counter = 0
    for i in data:
        counter += 1
        print str(counter) + " / " + str(total)
        query_string = ""

        data_keys = i.keys()
        for j in i.keys():
            key_clean = j.replace("/", "_").lower()
            data_keys.append ( key_clean )
            i[key_clean] = i[j]

        addAnd = False
        for k in keys.keys():
            if k in data_keys:
                if addAnd:
                    query_string += " AND "
                else:
                    addAnd = True
                query_string += "" + str(k) + "=E'" + unicode( i[k]  ).replace("'", "\\'") + "'"

        #check if it is already in the DB
        #print cur.mogrify("SELECT * FROM %s WHERE %s" , (table, query_string))
        cur.execute("SELECT * FROM test WHERE %s"  % (query_string))
        if cur.rowcount == 0:
            field_list = ""
            value_list = ""
            addAnd = False

            for k in keys.keys():
                if k in data_keys:
                    if addAnd:
                        field_list += ", "
                        value_list += ", "
                    else:
                        addAnd = True

                    field_list += "" + str(k) + ""
                    value_list += "E'" + unicode( i[k]  ).replace("'", "\\'") + "'"


            cur.execute("INSERT INTO test (%s) VALUES (%s);"  % (field_list, value_list ))

    conn.commit()


    conn.close()

if __name__ == "__main__":
    main(sys.argv[1:])
