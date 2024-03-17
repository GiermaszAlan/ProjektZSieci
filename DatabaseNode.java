import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

public class DatabaseNode {

    public static class ServerThread extends Thread{

        String[] args;

        public ServerThread(String[] args){
            this.args = args;
        }

        @Override
        public void run() {
            super.run();
try {


    int port = 0;
    String adress = "localhost";
    int k = 0;
    int v = 0;
    String adressConnect;
    int portConnect;
    ServerSocket server = null;

    ArrayList<String> nodesU = new ArrayList<String>();
    ArrayList<String> nodesD = new ArrayList<String>();





    for (int i = 0; i < args.length; i++) {
        switch (args[i]) {
            case "-tcpport":
                port = Integer.parseInt(args[++i]);
                server = new ServerSocket(port);

            System.out.println(adress + " : " + port);
                break;
            case "-record":
                String[] recordArray = args[++i].split(":");
                k = Integer.parseInt(recordArray[0]);
                v = Integer.parseInt(recordArray[1]);

                break;
            case "-connect":
                String connectS = args[++i];
                String[] connectArray = connectS.split(":");
                adressConnect = connectArray[0];
                portConnect = Integer.parseInt(connectArray[1]);
                try {

                    Socket socketS = new Socket(adressConnect, portConnect);
                    PrintWriter outS  = new PrintWriter(socketS.getOutputStream(), true);
                    outS.println("Connected: " + adress + ":" + port);
                    nodesU.add(connectS);
                    socketS.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }


                break;
        }
    }


    Socket socketClient;

    PrintWriter outC;
    BufferedReader inC;
    boolean t = true;

    while (t) {

            try {

                socketClient = server.accept();

                 outC = new PrintWriter(socketClient.getOutputStream(), true);
                 inC = new BufferedReader(new InputStreamReader(socketClient.getInputStream()));

                String odb = inC.readLine();
                System.out.println(odb);
                String[] s = odb.split(" ");

                switch (s[0]) {
                    case "Connected:": {

                        nodesD.add(s[1]);
                        socketClient.close();

                    }
                        break;
                    case "set-value":
                        String[] keyValueS = s[1].split(":");
                        int key1 = Integer.parseInt(keyValueS[0]);
                        int value1 = Integer.parseInt(keyValueS[1]);

                        if (key1 == k) {
                            v = value1;
                            outC.println("OK");
                            socketClient.close();
                        } else {


                            String ja = "hostToHostD " + s[0] + " " + s[1];

                            boolean d1 = true;

                                for (String socketString : nodesD) {
                                    String[] socketTab = socketString.split(":");
                                    Socket socketL = new Socket(socketTab[0], Integer.parseInt(socketTab[1]));
                                    PrintWriter outS = new PrintWriter(socketL.getOutputStream(), true);
                                    BufferedReader in = new BufferedReader(new InputStreamReader(socketL.getInputStream()));
                                    outS.println(ja);

                                    String response = in.readLine();

                                    switch (response) {
                                        case "OK":
                                            outC.println("OK");
                                            socketL.close();
                                            d1 = false;
                                            break;
                                        case "NOTme":
                                            socketL.close();
                                            continue;
                                    }
                                    if (!d1) {
                                        break;
                                    }
                                }


                            ja = "hostToHostU " + s[0] + " " + s[1];
                            boolean d2 = true;

                                for (String socketString : nodesU) {
                                    String[] socketTab = socketString.split(":");
                                    Socket socketL = new Socket(socketTab[0], Integer.parseInt(socketTab[1]));
                                    PrintWriter outS = new PrintWriter(socketL.getOutputStream(), true);
                                    BufferedReader in = new BufferedReader(new InputStreamReader(socketL.getInputStream()));
                                    outS.println(ja);
                                    String response = in.readLine();

                                    switch (response) {
                                        case "OK":
                                            outC.println("OK");
                                            socketL.close();
                                            d2 = false;
                                            break;
                                        case "NOTme":
                                            socketL.close();
                                            continue;
                                    }
                                    if (!d2) {
                                        break;
                                    }
                                }

                            if (d1 && d2) {
                                outC.println("ERROR");
                            }

                        }
                        break;


                    case "get-value":

                        if (Integer.parseInt(s[1]) == k) {
                            outC.println(s[1] + ":" + v);
                            socketClient.close();
                        } else {


                            String ja = "hostToHostD " + s[0] + " " + s[1];

                            boolean d1 = true;

                                for (String socketString : nodesD) {
                                    String[] socketTab = socketString.split(":");
                                    Socket socketL = new Socket(socketTab[0], Integer.parseInt(socketTab[1]));
                                    PrintWriter outS = new PrintWriter(socketL.getOutputStream(), true);
                                    BufferedReader in = new BufferedReader(new InputStreamReader(socketL.getInputStream()));
                                    outS.println(ja);
                                    String[] response = in.readLine().split(" ");

                                    switch (response[0]) {
                                        case "OK":
                                            outC.println(response[1]);
                                            socketClient.close();
                                            d1 = false;
                                            socketL.close();
                                            break;
                                        case "NOTme":
                                            socketL.close();
                                            continue;
                                    }
                                    if (!d1) {
                                        break;
                                    }


                            }

                            ja = "hostToHostU " + s[0] + " " + s[1];
                            boolean d2 = true;

                                for (String socketString : nodesU) {
                                    String[] socketTab = socketString.split(":");
                                    Socket socketL = new Socket(socketTab[0], Integer.parseInt(socketTab[1]));
                                    PrintWriter outS = new PrintWriter(socketL.getOutputStream(), true);
                                    BufferedReader in = new BufferedReader(new InputStreamReader(socketL.getInputStream()));
                                    outS.println(ja);
                                    String[] response = in.readLine().split(" ");

                                    switch (response[0]) {
                                        case "OK":
                                            outC.println(response[1]);
                                            socketClient.close();
                                            d2 = false;
                                            socketL.close();
                                            break;
                                        case "NOTme":
                                            socketL.close();
                                            continue;
                                    }
                                    if (!d2) {
                                        break;
                                    }
                                }


                            if (d1 && d2) {
                                outC.println("ERROR");
                                socketClient.close();
                            }

                        }


                        break;


                    case "find-key":
                        if (Integer.parseInt(s[1]) == k) {
                            outC.println(adress + ":" + port);
                            socketClient.close();
                        } else {


                            String ja = "hostToHostD " + s[0] +" "+ s[1];

                            boolean d1 = true;

                                for (String socketString : nodesD) {
                                    String[] socketTab = socketString.split(":");
                                    Socket socketL = new Socket(socketTab[0], Integer.parseInt(socketTab[1]));
                                    PrintWriter outS = new PrintWriter(socketL.getOutputStream(), true);
                                    BufferedReader in = new BufferedReader(new InputStreamReader(socketL.getInputStream()));
                                    outS.println(ja);
                                    String[] response = in.readLine().split(" ");

                                    switch (response[0]) {
                                        case "OK":
                                            outC.println(response[1]);
                                            socketL.close();
                                            d1 = false;
                                            break;
                                        case "NOTme":
                                            socketL.close();
                                            continue;
                                    }
                                    if (!d1) {
                                        break;
                                    }
                                }


                            ja = "hostToHostU " + s[0] +" "+ s[1];
                            boolean d2 = true;

                                for (String socketString : nodesU) {
                                    String[] socketTab = socketString.split(":");
                                    Socket socketL = new Socket(socketTab[0], Integer.parseInt(socketTab[1]));
                                    PrintWriter outS = new PrintWriter(socketL.getOutputStream(), true);
                                    BufferedReader in = new BufferedReader(new InputStreamReader(socketL.getInputStream()));
                                    outS.println(ja);
                                    String[] response = in.readLine().split(" ");

                                    switch (response[0]) {
                                        case "OK":
                                            outC.println(response[1]);
                                            socketClient.close();
                                            d2 = false;
                                            break;
                                        case "NOTme":
                                            socketClient.close();
                                            continue;
                                    }
                                    if (!d2) {
                                        break;
                                    }
                                }


                            if (d1 && d2) {
                                outC.println("ERROR");
                                socketClient.close();
                            }

                        }


                        break;

                    case "get-max" : {

                        int key = k;
                        int max = v;
                        String ans = key + ":" + max;
                        String ja = "hostToHostD " + s[0] +" "+ ans;


                            for (String socketString : nodesD) {
                                String[] socketTab = socketString.split(":");
                                Socket socketL = new Socket(socketTab[0], Integer.parseInt(socketTab[1]));
                                PrintWriter outS = new PrintWriter(socketL.getOutputStream(), true);
                                BufferedReader in = new BufferedReader(new InputStreamReader(socketL.getInputStream()));
                                outS.println(ja);
                                ans = in.readLine();
                                socketL.close();
                            }


                        ja = "hostToHostU " + s[0] +" "+ ans;

                            for (String socketString : nodesU) {
                                String[] socketTab = socketString.split(":");
                                Socket socketL = new Socket(socketTab[0], Integer.parseInt(socketTab[1]));
                                PrintWriter outS = new PrintWriter(socketL.getOutputStream(), true);
                                BufferedReader in = new BufferedReader(new InputStreamReader(socketL.getInputStream()));
                                outS.println(ja);
                                ans = in.readLine();
                                socketL.close();
                            }


                        outC.println(ans);
                        socketClient.close();
                    }
                    break;
                    case "get-min": {

                        int keym = k;
                        int min = v;
                        String ans = keym + ":" + min;
                        String ja = "hostToHostD " + s[0] +" "+ ans;

                            for (String socketString : nodesD) {
                                String[] socketTab = socketString.split(":");
                                Socket socketL = new Socket(socketTab[0], Integer.parseInt(socketTab[1]));
                                PrintWriter outS = new PrintWriter(socketL.getOutputStream(), true);
                                BufferedReader in = new BufferedReader(new InputStreamReader(socketL.getInputStream()));
                                outS.println(ja);
                                ans = in.readLine();
                                socketL.close();
                            }

                        System.out.print("");
                        ja = "hostToHostU " + s[0] +" "+ ans;

                            for (String socketString : nodesU) {
                                String[] socketTab = socketString.split(":");
                                Socket socketL = new Socket(socketTab[0], Integer.parseInt(socketTab[1]));
                                PrintWriter outS = new PrintWriter(socketL.getOutputStream(), true);
                                BufferedReader in = new BufferedReader(new InputStreamReader(socketL.getInputStream()));
                                outS.println(ja);
                                ans = in.readLine();
                                socketL.close();
                            }


                        outC.println(ans);
                        socketClient.close();
                    }
                    break;
                    case "new-record":
                        String[] newKV = s[1].split(":");

                        k = Integer.parseInt(newKV[0]);
                        v = Integer.parseInt(newKV[1]);

                        outC.println("OK");

                        break;
                    case "terminate":



                            for (String socketString : nodesD) {
                                String[] socketTab = socketString.split(":");
                                Socket socketL = new Socket(socketTab[0], Integer.parseInt(socketTab[1]));
                                PrintWriter outS = new PrintWriter(socketL.getOutputStream(), true);
                                outS.println("hostToHostImTD "+port);
                            }


                            for (String socketString : nodesU) {
                                String[] socketTab = socketString.split(":");
                                Socket socketL = new Socket(socketTab[0], Integer.parseInt(socketTab[1]));
                                PrintWriter outS = new PrintWriter(socketL.getOutputStream(), true);
                                outS.println("hostToHostImTU " +port);
                            }


                        outC.println("OK");
                        socketClient.close();
                        t = false;


                        break;





                    //////////////////////////////////////////////








                    case "hostToHostImTD": {

                        for (int i=0;i<nodesU.size();i++) {
                            String[] socketTab = nodesU.get(i).split(":");
                            if (Integer.parseInt(socketTab[1]) == Integer.parseInt(s[1])) {
                                System.out.println("Usuwam " + s[1]);
                                nodesU.remove(i);
                                break;
                            }
                        }
                        socketClient.close();
                    }
                    break;
                    case "hostToHostImTU": {
                        for (int i=0;i<nodesD.size();i++) {
                            String[] socketTab = nodesD.get(i).split(":");

                            if (Integer.parseInt(socketTab[1]) == Integer.parseInt(s[1])) {
                                System.out.println("Usuwam " + s[1]);
                                nodesD.remove(i);
                                break;
                            }
                        }
                        socketClient.close();
                    }
                    break;


                    case "hostToHostD":
                        switch (s[1]) {
                            case "set-value":
                                String[] kvSVd = s[2].split(":");
                                if (Integer.parseInt(kvSVd[0]) == k) {
                                    v = Integer.parseInt(kvSVd[1]);

                                    outC.println("OK");
                                } else {


                                    boolean d = true;

                                        for (String socketString : nodesD) {
                                            String[] socketTab = socketString.split(":");
                                            Socket socketL = new Socket(socketTab[0], Integer.parseInt(socketTab[1]));
                                            PrintWriter outS = new PrintWriter(socketL.getOutputStream(), true);
                                            BufferedReader in = new BufferedReader(new InputStreamReader(socketL.getInputStream()));
                                            outS.println(odb);
                                            String response = in.readLine();

                                            switch (response) {
                                                case "OK":
                                                    outC.println("OK");
                                                    socketL.close();
                                                    d = false;
                                                    break;
                                                case "NOTme":
                                                    continue;
                                            }
                                            if (!d) {
                                                break;
                                            }
                                        }

                                    if (d)
                                        outC.println("NOTme");
                                }

                                break;
                            case "get-value":
                                if (Integer.parseInt(s[2]) == k) {
                                    outC.println("OK " + k + ":" + v);
                                } else {


                                    boolean d = true;

                                        for (String socketString : nodesD) {
                                            String[] socketTab = socketString.split(":");
                                            Socket socketL = new Socket(socketTab[0], Integer.parseInt(socketTab[1]));
                                            PrintWriter outS = new PrintWriter(socketL.getOutputStream(), true);
                                            BufferedReader in = new BufferedReader(new InputStreamReader(socketL.getInputStream()));
                                            outS.println(odb);
                                            String response = in.readLine();
                                            String[] responseT = response.split(" ");



                                            switch (responseT[0]) {
                                                case "OK":
                                                    outC.println(response);
                                                    d = false;
                                                    break;
                                                case "NOTme":
                                                    continue;
                                            }
                                            if (!d) {
                                                break;
                                            }
                                        }

                                    if (d)
                                        outC.println("NOTme");
                                }
                                break;
                            case "find-key":
                                if (Integer.parseInt(s[2]) == k) {
                                    outC.println("OK " + adress + ":" + port);
                                } else {


                                    boolean d = true;

                                        for (String socketString : nodesD) {
                                            String[] socketTab = socketString.split(":");
                                            Socket socketL = new Socket(socketTab[0], Integer.parseInt(socketTab[1]));
                                            PrintWriter outS = new PrintWriter(socketL.getOutputStream(), true);
                                            BufferedReader in = new BufferedReader(new InputStreamReader(socketL.getInputStream()));
                                            outS.println(odb);
                                            String response = in.readLine();
                                            String[] responseT = response.split(" ");

                                            switch (responseT[0]) {
                                                case "OK":
                                                    outC.println(response);
                                                    d = false;
                                                    break;
                                                case "NOTme":
                                                    continue;
                                            }
                                            if (!d) {
                                                break;
                                            }
                                        }

                                    if (d)
                                        outC.println("NOTme");
                                }
                                break;
                            case "get-max": {
                                String o = s[2];
                                String[] kv = s[2].split(":");
                                if (Integer.parseInt(kv[1]) < v) {
                                 o = k+ ":"+v;
                                }


                                    for (String socketString : nodesD) {
                                        String[] socketTab = socketString.split(":");
                                        Socket socketL = new Socket(socketTab[0], Integer.parseInt(socketTab[1]));
                                        PrintWriter outS = new PrintWriter(socketL.getOutputStream(), true);
                                        BufferedReader in = new BufferedReader(new InputStreamReader(socketL.getInputStream()));
                                        outS.println(odb);
                                        o = in.readLine();
                                        socketL.close();
                                    }


                                outC.println(o);

                            }
                            break;
                            case "get-min": {
                                String o = s[2];
                                String[] kv = s[2].split(":");
                                if (Integer.parseInt(kv[1]) > v) {
                                    //    outC.println(k + ":" + v);
                                    o = k+ ":"+v;
                                }


                                    for (String socketString : nodesD) {
                                        String[] socketTab = socketString.split(":");
                                        Socket socketL = new Socket(socketTab[0], Integer.parseInt(socketTab[1]));
                                        PrintWriter outS = new PrintWriter(socketL.getOutputStream(), true);
                                        BufferedReader in = new BufferedReader(new InputStreamReader(socketL.getInputStream()));
                                        outS.println(odb);
                                        o = in.readLine();
                                        socketL.close();
                                    }


                                outC.println(o);

                            }
                            break;

                        }


                        break;


                    /////////////////////////////////////////////////


                    case "hostToHostU":
                        switch (s[1]) {
                            case "set-value":
                                String[] kvSVU = s[2].split(":");

                                if (Integer.parseInt(kvSVU[0]) == k) {
                                    v = Integer.parseInt(kvSVU[1]);

                                    outC.println("OK");
                                } else {


                                    boolean d = true;

                                        for (String socketString : nodesU) {
                                            String[] socketTab = socketString.split(":");
                                            Socket socketL = new Socket(socketTab[0], Integer.parseInt(socketTab[1]));
                                            PrintWriter outS = new PrintWriter(socketL.getOutputStream(), true);
                                            BufferedReader in = new BufferedReader(new InputStreamReader(socketL.getInputStream()));
                                            outS.println(odb);
                                            String response = in.readLine();

                                            switch (response) {
                                                case "OK":
                                                    outC.println("OK");
                                                    socketL.close();
                                                    d = false;
                                                    break;
                                                case "NOTme":
                                                    continue;
                                            }
                                            if (!d) {
                                                break;
                                            }
                                        }

                                    if (d)
                                        outC.println("NOTme");
                                }
                                break;
                            case "get-value":
                                if (Integer.parseInt(s[2]) == k) {
                                    outC.println("OK " + k + ":" + v);
                                } else {


                                    boolean d = true;

                                        for (String socketString : nodesU) {
                                            String[] socketTab = socketString.split(":");
                                            Socket socketL = new Socket(socketTab[0], Integer.parseInt(socketTab[1]));
                                            PrintWriter outS = new PrintWriter(socketL.getOutputStream(), true);
                                            BufferedReader in = new BufferedReader(new InputStreamReader(socketL.getInputStream()));
                                            outS.println(odb);
                                            String response = in.readLine();
                                            String[] responseT = response.split(" ");

                                            switch (responseT[0]) {
                                                case "OK":
                                                    outC.println(response);
                                                    d = false;
                                                    break;
                                                case "NOTme":
                                                    continue;
                                            }
                                            if (!d) {
                                                break;
                                            }
                                        }

                                    if (d)
                                        outC.println("NOTme");
                                }
                                break;
                            case "find-key":
                                if (Integer.parseInt(s[2]) == k) {
                                    outC.println("OK " + adress + ":" + port);
                                } else {


                                    boolean d = true;

                                        for (String socketString : nodesU) {
                                            String[] socketTab = socketString.split(":");
                                            Socket socketL = new Socket(socketTab[0], Integer.parseInt(socketTab[1]));
                                            PrintWriter outS = new PrintWriter(socketL.getOutputStream(), true);
                                            BufferedReader in = new BufferedReader(new InputStreamReader(socketL.getInputStream()));
                                            outS.println(odb);
                                            String response = in.readLine();
                                            String[] responseT = response.split(" ");

                                            switch (responseT[0]) {
                                                case "OK":
                                                    outC.println(response);
                                                    d = false;
                                                    break;
                                                case "NOTme":
                                                    continue;
                                            }
                                            if (!d) {
                                                break;
                                            }
                                        }
                                        if (d)
                                            outC.println("NOTme");
                                    }

                                break;
                            case "get-max": {
                                String o = s[2];
                                String[] kv = s[2].split(":");
                                if (Integer.parseInt(kv[1]) < v) {
                                    o = k+ ":"+v;
                                }


                                    for (String socketString : nodesU) {
                                        String[] socketTab = socketString.split(":");
                                        Socket socketL = new Socket(socketTab[0], Integer.parseInt(socketTab[1]));
                                        PrintWriter outS = new PrintWriter(socketL.getOutputStream(), true);
                                        BufferedReader in = new BufferedReader(new InputStreamReader(socketL.getInputStream()));
                                        outS.println(odb);
                                        o = in.readLine();
                                        socketL.close();
                                    }


                                outC.println(o);


                            }break;
                            case "get-min": {
                                String o = s[2];
                                String[] kv = s[2].split(":");
                                if (Integer.parseInt(kv[1]) > v) {

                                    o = k+ ":"+v;
                                }


                                    for (String socketString : nodesU) {
                                        String[] socketTab = socketString.split(":");
                                        Socket socketL = new Socket(socketTab[0], Integer.parseInt(socketTab[1]));
                                        PrintWriter outS = new PrintWriter(socketL.getOutputStream(), true);
                                        BufferedReader in = new BufferedReader(new InputStreamReader(socketL.getInputStream()));
                                        outS.println(odb);
                                        o = in.readLine();
                                        socketL.close();
                                    }


                                outC.println(o);

                            }

break;
                        }


                }

            } catch (IOException e) {
                e.printStackTrace();
            }

        }


    } catch(IOException e){
        e.printStackTrace();
    }


        }
    }





    public static void main(String[] args) throws IOException {

       ServerThread server = new ServerThread(args);
       server.start();


    }






}
