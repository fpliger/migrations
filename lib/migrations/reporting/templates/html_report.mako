Migration analysis:

Source: ${source}
Destination: ${destination}
Transfer Mode: ${transfer_mode}

Total Records Transferred: ${total_records_transferred}
Total Records Skipped: ${total_records_skipped}

=================================
Exceptions Occurred:
=================================
%for exc in exceptions:
    ${exc}
%endfor


=================================
Global Messages:
=================================
%for exc in messages:
    ${exc}
%endfor


=================================
Tables Transfer Details:
=================================
%for table, stats in tables.items():
    %if not stats["skipped"] and stats["records_transferred"]>0:
        ---------------------------------
        ${table}
        ---------------------------------
        Pk MAP: ${stats["pk_map"]}
        Records To Transfer: ${stats["records_transferred"]}
        Records Transferred: ${stats["records_transferred"]}
        Exceptions Occurred: ${len(stats["messages"])}
        %if stats["messages"]:
            Exceptions Details
            %for record in stats["messages"]:
                ${record}
            %endfor
        %endif
        Exceptions Occurred: ${len(stats["exceptions"])}

        %if stats["exceptions"]:
            Exceptions Details
            %for record in stats["exceptions"]:
                ${record}
            %endfor
        %endif

        Records Transferred Details:
        %if transfer_mode == "DIFF":
            %for record in stats["lst_records_transferred"]:
                ${record}
            %endfor
        %endif
        ---------------------------------

    %endif
%endfor

Tables Dependecies:

