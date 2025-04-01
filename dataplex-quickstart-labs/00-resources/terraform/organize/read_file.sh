#!/bin/bash

temp_file=$(mktemp)

while IFS= read -r line; do
  line=$(echo "$line" | xargs) # Remove leading/trailing whitespace

  if [[ -z "$line" || "$line" == \#* ]]; then
    continue # Skip empty or comment lines
  fi

  # Extract all parent directories using regex
  while [[ "$line" =~ ^(.*)/[^/]+$ ]]; do
    dir="${BASH_REMATCH[1]}"
    if [[ ! "$dir" =~ ^gs:\/$ ]]; then
      echo "$dir" >> $temp_file    
    fi

    line="$dir" # Update line to the parent directory for next iteration
  done

  # Handle the case where the input line is a directory itself (no '/')
  if [[ ! "$line" =~ / ]]; then # If no slash present, print the input
    echo "$line" >> $temp_file
  fi

done < $1 | sort | uniq # Sort and remove duplicates *after* processing all lines


file_content=$(cat "$temp_file")
echo '{"file_content":"'$file_content'"}' # Output text
