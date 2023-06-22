-module(algo).
-export([start/3,line/5,server/3,worker/3,broadcast/2,pushsum/6]).

start(Nodes,Topology,Algo) ->

    case (Topology == twoDimension) or (Topology == imPerfect3D) of 
        true -> 
            GridMatrixsize = trunc(math:sqrt(Nodes)),
            register(server,spawn(algo,server,[[],0,GridMatrixsize*GridMatrixsize])),
            register(grid,spawn(algo,grid,[GridMatrixsize, array:new(GridMatrixsize), Nodes, GridMatrixsize,Topology,Algo]));
        _ ->
            register(server,spawn(algo,server,[[],0,Nodes])),  
            register(line,spawn(algo,line,[Nodes,array:new(Nodes),Topology,Algo,Nodes]))
    end.

server(Neighbours,N,Nodes) ->
    receive 
    {nodes,Nodes} ->
        io:format("Nodes:~p~n",[array:to_list(Nodes)]),
        server(Neighbours,N,Nodes);
    {rumor,Id} ->
        io:format("Worker count ~p coverged, Total:~p~n",[Id,N+1]),
        if 
            N == Nodes-1 -> 
                {_,Time} = statistics(wall_clock),
                io:format("Total Time Elapsed:~p~n",[Time]);
            true -> server(Neighbours,N+1,Nodes)
        end
    end.

worker(N,Id,R) ->
    receive
        {neigbours,Neighbour} ->
            io:format("Neighbours of particular worker ~p:~p~n",[Id,Neighbour]),
            worker(Neighbour,Id,R);
        {fullneigbours,Neighbour} ->
            worker(Neighbour,Id,R);
        {start} ->
            if 
                R == 10 -> 
                    server ! {rumor,Id},
                    exit(normal);
                true ->
                    broadcast(N,10),
                    worker(N,Id,R+1)
            end                             
    end.

pushsum(N,S,W,C,Id,Converged) ->
    receive
        {neigbours,Neighbour} ->
            io:format("Neighbours of particular worker ~p:~p~n",[Id,Neighbour]),
            pushsum(Neighbour,Id,S,W,C,Converged);
	    {fullneigbours,Neighbour} ->	
            pushsum(Neighbour,Id,S,W,C,Converged);        

        {start} ->
            Rand = rand:uniform(length(N)),
            Pid = lists:nth(Rand,N),
            Pid ! {pushsum,S,W},
            pushsum(N,S,W,C,Id,Converged);

        {pushsum,Sum,Weight} ->
            
            NewSvalue = S + Sum,
            NewWvalue = W + Weight,
            Diff = abs(S/W - NewSvalue/NewWvalue),
            case Converged == tru of
                true ->
                    Rand = rand:uniform(length(N)),
                    Pid = lists:nth(Rand,N),
                    Pid ! {pushsum,NewSvalue/2,NewWvalue/2},
                    pushsum(N,Id,NewSvalue/2,NewWvalue/2,C,Converged);
                _ ->    
                    case Diff < math:pow(10,-10) of
                true -> 
                    if 
                        C+1 == 3 ->
                            Rand = rand:uniform(length(N)),
                            Pid = lists:nth(Rand,N),
                            Pid ! {pushsum,NewSvalue/2,NewWvalue/2}, 
                            server ! {rumor,Id},
                            pushsum(N,Id,NewSvalue/2,NewWvalue/2,C,tru);
                        true -> 
                            Rand = rand:uniform(length(N)),
                            Pid = lists:nth(Rand,N),
                            broadcastsum(N,20,NewSvalue/2,NewWvalue/2),
                            Pid ! {pushsum,NewSvalue/2,NewWvalue/2},
                            pushsum(N,Id,NewSvalue/2,NewWvalue/2,C+1,Converged)
                    end;
                _ ->
                    Rand = rand:uniform(length(N)),
                    Pid = lists:nth(Rand,N),
                    Pid ! {pushsum,NewSvalue/2,NewWvalue/2},
                    pushsum(N,Id,NewSvalue/2,NewWvalue/2,0,Converged)
            end    
        end
    end.

broadcastsum(N,0,S,W) ->
    ok;
broadcastsum(N,C,S,W) ->
    Rand = rand:uniform(length(N)),
    Pid = lists:nth(Rand,N),
    case is_process_alive(Pid) of 
        true -> 
            Pid ! {pushsum,S,W},
            broadcast(N,C-1);
        _ -> broadcast(N,C-1)
    end.


broadcast(N,0) ->
    ok;
broadcast(N,C) ->
    Rand = rand:uniform(length(N)),
    Pid = lists:nth(Rand,N),
    case is_process_alive(Pid) of 
        true -> 
            Pid ! {start},
            broadcast(N,C-1);
        _ -> broadcast(N,C-1)
    end.


line(0,Array,Topology,Algo,Nodes)  ->
    server ! {nodes,Array},
    if 
        Topology == line ->
            neighbourLine(Array,0);
        Topology == full ->
            neighbourFull(Array,0);
        true -> ok
    end,
    {_,_} = statistics(wall_clock),
    Rand = rand:uniform(array:size(Array)-1),
    Pid = array:get(Rand,Array),
    Pid ! {start};
line(N,Array,Topolgy,Algo,Nodes) ->
    case Algo == gossip of
        true -> 
            Pid = spawn(algo,worker,[[],N-1,0]), 
            ArrayN =  array:set(N-1,Pid,Array),
            ArrayN =  array:set(N-1,Pid,Array),
            line(N-1,ArrayN,Topolgy,Algo,Nodes);
        _ -> 
            Pid = spawn(algo,pushsum,[[],N-1,N-1,1,0,fal]),
            ArrayN =  array:set(N-1,Pid,Array),
            ArrayN =  array:set(N-1,Pid,Array),
            line(N-1,ArrayN,Topolgy,Algo,Nodes)
    end.

neighbourLine(Array,N) ->
    Neighbour = [],
    Size = array:size(Array),
    if 
        N == Size-1 ->
            Neighbourn = lists:append(Neighbour,[array:get(N-1,Array)]),
            array:get(N,Array) ! {neigbours,Neighbourn};
        N == 0 -> 
            Neighbourn = lists:append(Neighbour,[array:get(N+1,Array)]),
            array:get(N,Array) ! {neigbours,Neighbourn};        
        true ->
            Neighbourn = lists:append(Neighbour,[array:get(N+1,Array),array:get(N-1,Array)]),
            array:get(N,Array) ! {neigbours,Neighbourn}
    end,
    case N == Size-1 of
        true -> ok;
        false -> neighbourLine(Array,N+1)

    end.
neighbourFull(Array,N) ->
    Size = array:size(Array),
    array:get(N,Array) ! {fullneigbours,lists:delete(array:get(N,Array),array:to_list(Array))},
    case N == Size-1 of
        true -> ok;
        false -> neighbourFull(Array,N+1)

    end.
neighbour2D(FinalArray,N, GridMatrixsize,Topology) ->
    Neighbour = [],
    
    I = trunc(N/GridMatrixsize),
    J = N rem GridMatrixsize,

    if 

        (I == (GridMatrixsize-1)) and (J >= 1) and (J =< (GridMatrixsize - 2)) ->
            Neighbourn = lists:append(Neighbour,[array:get(J+1,array:get(I, FinalArray)), array:get(J-1,array:get(I, FinalArray)), array:get(J,array:get(I-1, FinalArray))]),
            twoD(FinalArray,I,J,Neighbourn,Topology);

        (I == 0) and (j == GridMatrixsize-1)  ->
            Neighbourn = lists:append(Neighbour,[array:get(J-1,array:get(I, FinalArray)), array:get(I,array:get(J+1, FinalArray))]),
            twoD(FinalArray,I,J,Neighbourn,Topology);

        (I == GridMatrixsize-1) and (J == GridMatrixsize-1)  ->
            Neighbourn = lists:append(Neighbour,[array:get(J,array:get((I-1), FinalArray)), array:get(J-1,array:get(I, FinalArray))]),
           twoD(FinalArray,I,J,Neighbourn,Topology);

        (I == 0) and (J >= 1) and (J =< (GridMatrixsize - 2)) ->
            Neighbourn = lists:append(Neighbour,[array:get(J+1,array:get(I, FinalArray)), array:get(J-1,array:get(I, FinalArray)), array:get(J,array:get(I+1, FinalArray))]),
            twoD(FinalArray,I,J,Neighbourn,Topology);

        (I >= 1) and (J == 0) and (I =< (GridMatrixsize - 2)) ->
            Neighbourn = lists:append(Neighbour,[array:get(J+1,array:get(I, FinalArray)), array:get(J,array:get(I-1, FinalArray)), array:get(J,array:get(I+1, FinalArray))]),
            twoD(FinalArray,I,J,Neighbourn,Topology);

        (I == GridMatrixsize-1) and (J == 0)  ->
            Neighbourn = lists:append(Neighbour,[array:get(J+1,array:get(I, FinalArray)), array:get(J,array:get(I-1, FinalArray))]),
            twoD(FinalArray,I,J,Neighbourn,Topology);

        (I >= 1) and (J == (GridMatrixsize - 1)) and (I =< (GridMatrixsize - 2)) ->
            Neighbourn = lists:append(Neighbour, [array:get(J-1,array:get(I, FinalArray)), array:get(J,array:get(I-1, FinalArray)), array:get(J,array:get(I+1, FinalArray))]),
            twoD(FinalArray,I,J,Neighbourn,Topology);

        (I == 0) and (J == 0)  ->
            Neighbourn = lists:append(Neighbour,[array:get(J+1,array:get(I, FinalArray)), array:get(J,array:get(I+1, FinalArray))]),
           twoD(FinalArray,I,J,Neighbourn,Topology);

        (I >= 1) and (J >= 1) and (I =< (GridMatrixsize - 2)) and (J =< (GridMatrixsize - 2)) ->
            Neighbourn = lists:append(Neighbour,[array:get(J+1,array:get(I, FinalArray)), array:get(J-1,array:get(I, FinalArray)), array:get(J,array:get(I-1, FinalArray)), array:get(J,array:get(I+1, FinalArray))]),
            twoD(FinalArray,I,J,Neighbourn,Topology);          

        true ->
            ok
    end,
    case N == 0 of
        true -> ok;
        false -> neighbour2D(FinalArray,N-1, GridMatrixsize,Topology)

    end.

twoD(FinalArray,I,J,Neighbourn,Topology) ->
    case Topology == imptwoDimension of
                true -> 
                    ArrayList = lists:foldl(fun(X,B)-> lists:append(B,array:to_list(X)) end, [],array:to_list(FinalArray)),
                    ImpNeighbours = lists:subtract(ArrayList,Neighbourn),                 
                    NewNeighbours = lists:append(Neighbourn,[lists:nth(rand:uniform(length(ImpNeighbours)),ImpNeighbours)]),
                    array:get(J, array:get(I, FinalArray))! {neigbours,NewNeighbours};
                _ ->
                    array:get(J, array:get(I, FinalArray))! {neigbours,Neighbourn}
    end.

grid(0, FinalArray, Number, GridMatrixsize,Topology,Algo)  ->
    server ! {nodes,FinalArray},
    neighbour2D(FinalArray, Number -1, GridMatrixsize,Topology),
    array:get(0,array:get(0, FinalArray)) ! {start};
 
grid(Num, FinalArray, Number, GridMatrixsize,Topology,Algo) ->

    Column =  gridfinal(GridMatrixsize, Num, array:new(GridMatrixsize), GridMatrixsize,Topology,Algo),
    ArrayN = array:set(Num-1, Column, FinalArray),
    grid(Num-1, ArrayN, Number, GridMatrixsize,Topology,Algo ).

gridfinal(0, Row, FinalArray, GridMatrixsize,Topology,Algo)  ->
    FinalArray;
 
gridfinal(Num,Row, FinalArray, GridMatrixsize,Topology,Algo) ->
    case Algo == gossip of	
        true ->	
            Pid = spawn(project2,worker,[[],(GridMatrixsize*(Row-1)) + (Num-1),0]),	
            ArrayNew =  array:set(Num-1,Pid, FinalArray),	
            gridfinal(Num-1,Row, ArrayNew, GridMatrixsize,Topology,Algo);	
        _ -> 	
            Pid = spawn(project2,push_worker,[[],(GridMatrixsize*(Row-1)) + (Num-1),(GridMatrixsize*(Row-1)) + (Num-1),1,0,fal]),	
            ArrayNew =  array:set(Num-1,Pid, FinalArray),	
            gridfinal(Num-1,Row, ArrayNew, GridMatrixsize,Topology,Algo)	
    end.