%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2015 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_exchange_type_shell).
-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").

-behaviour(rabbit_exchange_type).

-export([description/0, serialise_events/0, route/2]).
-export([validate/1, validate_binding/2,
         create/2, delete/3, policy_changed/2, add_binding/3,
         remove_bindings/3, assert_args_equivalence/2]).

-rabbit_boot_step({?MODULE,
                   [{description, "exchange type shell"},
                    {mfa,         {rabbit_registry, register,
                                   [exchange, <<"x-shell">>, ?MODULE]}},
                    {requires,    rabbit_registry},
                    {enables,     kernel_ready}]}).

-define(HEADER_NAME_QC, "QC").
-define(HEADER_VALUE_VALID, "valid").
-define(HEADER_VALUE_INVALID, "invalid").
-define(EXCHANGE_SUFFIX_INVALID, "invalid").

description() ->
    [{description, <<"customized exchange">>}].

serialise_events() -> false.

mark_invalid(Content = #content{properties = Properties}) ->
    Headers = case Properties#'P_basic'.headers of
                  undefined -> [];
                  H         -> H
              end,
    NewHeaders = [{<<?HEADER_NAME_QC>>, longstr, <<?HEADER_VALUE_INVALID>>} | Headers],
    NewProperties = Properties#'P_basic'{headers = NewHeaders},
    Content#content{properties = NewProperties}.

add_invalid_exchange_suffix(RoutingKeys) ->
    [ <<K/binary, <<".">>/binary, <<?EXCHANGE_SUFFIX_INVALID>>/binary>> || K <- RoutingKeys ].

divert_invalid(RoutingKeys) ->
    add_invalid_exchange_suffix(RoutingKeys).

exec_shell_cmd(Cmd) ->
    CmdOut = os:cmd(Cmd),
    rabbit_log:info("~p~n", [CmdOut]),
    case string:equal(CmdOut, "OK") of
        true ->
            valid;
        false ->
            invalid
    end.

%%write_to_namedpipe(PayloadFragments)->
%%    write_to_namedpipe_fragment(PayloadFragments)


validate_content(Content = #content{properties = _Properties}) ->
    Cmd = string:concat("echo 'OK", "' | ./validate_payload"),
    rabbit_log:info("~p~n", [Cmd]),
    rabbit_log:info("~p~n", [Content#content.payload_fragments_rev]),
    Result = exec_shell_cmd(Cmd),
    case Result of
        invalid ->
            {Result, mark_invalid(Content)};
        _ ->
            {Result, Content}
    end.

validate_message(Message = #basic_message{routing_keys = RoutingKeys, content = Content}) ->
    {Result, NewContent} = validate_content(Content),
    case Result of
        invalid ->
            NewRoutingKeys = divert_invalid(RoutingKeys),
            NewMessage = Message#basic_message{routing_keys = NewRoutingKeys, content = NewContent},
            {Result, NewMessage};
        _ ->
            {Result, Message}
    end.

route(Exchange = #exchange{name = _X},
      Delivery = #delivery{message = Msg}) ->
    {Result, NewMsg} = validate_message(Msg),
    case Result of
        invalid ->
            NewDelivery = Delivery#delivery{message = NewMsg},
            rabbit_exchange_type_topic:route(Exchange, NewDelivery);
        _ ->
            rabbit_exchange_type_topic:route(Exchange, Delivery)
    end.

validate(_X) -> ok.

validate_binding(X, B) ->
    rabbit_exchange_type_topic:validate_binding(X, B).

create(_Tx, _X) -> ok.

delete(Tx, X, Bs) ->
    rabbit_exchange_type_topic:delete(Tx, X, Bs).

policy_changed(_X1, _X2) -> ok.

add_binding(Tx, X, B) ->
    rabbit_exchange_type_topic:add_binding(Tx, X, B).

remove_bindings(Tx, X, Bs) ->
    rabbit_exchange_type_topic:remove_bindings(Tx, X, Bs).

assert_args_equivalence(X, Args) ->
    rabbit_exchange:assert_args_equivalence(X, Args).

