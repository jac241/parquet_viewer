require 'glimmer-dsl-libui'
require 'polars'

class ParquetViewer
  include Glimmer

  PAGE_SIZE = 500

  def initialize
    @df = nil
    @headers = []
    @total_rows = 0
    @current_offset = 0

    # --- CHANGE 1: Add a variable to hold the table proxy ---
    @table_proxy = nil
    
    @status_label = nil
    @prev_button = nil
    @next_button = nil
  end

  def launch
    window('Parquet Viewer', 800, 600) {
      margined true

      vertical_box {
        horizontal_box {
          stretchy false

          button('Open Parquet File') {
            on_clicked do
              file = open_file
              load_parquet_data(file) if file && File.extname(file).downcase == '.parquet'
            end
          }

          @prev_button = button('Previous') {
            enabled false
            on_clicked { go_previous }
          }

          @next_button = button('Next') {
            enabled false
            on_clicked { go_next }
          }
          
          @status_label = label('No file loaded.') { stretchy true }
        }

        # This container will hold our table. It starts empty.
        @table_container = vertical_box {
          stretchy true
        }
      }
    }.show
  end

  private

  # Loads the file, creates the table structure ONCE, and displays the first page.
  def load_parquet_data(file_path)
    begin
      @df = Polars.read_parquet(file_path)
      @headers = @df.columns
      @total_rows = @df.height
      @current_offset = 0
      
      # --- CHANGE 2: Create the table structure just one time ---
      # Clear any previous table before creating a new one.
      @table_container.content {
        # Store the proxy for the table widget itself
        @table_proxy = table {
          stretchy true
          editable false
          
          # The columns are now defined only once when a new file is loaded.
          @headers.each { |header| text_column(header) }
        }
      }
      
      # Now, populate the newly created table with the first page of data.
      update_table_view
      
    rescue => e
      msg_box_error("Error Reading File", "An error occurred: #{e.message}")
      @df = nil # Clear dataframe on error
    end
  end

  # This method now ONLY updates the data in the existing table.
  def update_table_view
    return unless @df && @table_proxy # Don't do anything if no file is loaded

    # 1. Slice the DataFrame.
    page_df = @df.slice(@current_offset, PAGE_SIZE)

    # 2. Convert *only the slice* to Ruby arrays.
    page_data = page_df.to_a.map do |row_hash|
      row_hash.values_at(*@headers)
    end

    # --- CHANGE 3: The Fix! Use data binding to update the rows ---
    # This simply replaces the data in the existing table instead of
    # creating a new one.
    @table_proxy.cell_rows = page_data
    
    # 4. Update the status label and button states.
    update_status
  end

  def update_status
    return unless @df && @total_rows > 0

    start_row = @current_offset + 1
    end_row = [@current_offset + PAGE_SIZE, @total_rows].min
    @status_label.text = "Showing Rows: #{start_row} - #{end_row} of #{@total_rows}"
    
    @prev_button.enabled = (@current_offset > 0)
    @next_button.enabled = (@current_offset + PAGE_SIZE < @total_rows)
  end

  def go_next
    return unless @df
    if @current_offset + PAGE_SIZE < @total_rows
      @current_offset += PAGE_SIZE
      update_table_view
    end
  end

  def go_previous
    return unless @df
    if @current_offset > 0
      @current_offset -= PAGE_SIZE
      @current_offset = [@current_offset, 0].max
      update_table_view
    end
  end
end

ParquetViewer.new.launch
