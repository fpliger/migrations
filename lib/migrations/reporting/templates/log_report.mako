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
Tables Transfer Details:
=================================
%for table, stats in tables.items():
    ---------------------------------
    ${table}
    Pk MAP: ${stats["pk_map"]}
    Records Transferred: ${stats["records_transferred"]}
    Records Skipped: ${stats["records_skipped"]}
    Exceptions Occurred: ${stats["exceptions"]}
    
    %if transfer_mode == "DIFF":
        %for record in stats["lst_records_transferred"]:
            ${rec}
        %endfor
    %endif
    ---------------------------------
    
%endfor

Tables Dependecies:

