require 'glimmer-dsl-libui'
require 'polars'

class ParquetViewer
  include Glimmer

  PAGE_SIZE = 500
  # --- CONFIGURATION ---
  # Change this value to make the left-side column list wider or narrower.
  # This is the best alternative to a draggable splitter in the libui toolkit.
  COLUMN_LIST_WIDTH = 450

  def initialize
    @df = nil
    
    # State management
    @original_headers = []
    @display_headers = []
    @total_rows = 0
    @current_offset = 0

    # UI elements
    @main_table_container = nil
    @main_table_proxy = nil
    @column_list_proxy = nil
    @status_label = nil
    @prev_button = nil
    @next_button = nil
  end

  def launch
    window('Parquet Viewer', 1000, 700) {
      margined true
      horizontal_box {
        # --- Left panel: Column List ---
        vertical_box {
          stretchy false
          label('Columns') { stretchy false }
          
          @column_list_proxy = table {
            text_column('Columns', width: COLUMN_LIST_WIDTH)
            editable false
            
            on_selection_changed do
              selection_index = @column_list_proxy.selection
              handle_column_selection(selection_index)
            end
          }
        }
        
        # --- Right panel: Controls and Main Table ---
        vertical_box {
          stretchy true
          horizontal_box {
            stretchy false
            button('Open Parquet File') { on_clicked { open_and_load_file } }
            @prev_button = button('Previous') { enabled false; on_clicked { go_previous } }
            @next_button = button('Next') { enabled false; on_clicked { go_next } }
            @status_label = label('No file loaded.') { stretchy true }
          }
          
          @main_table_container = vertical_box { stretchy true }
        }
      }
    }.show
  end

  private

  def open_and_load_file
    file = open_file
    return unless file && File.extname(file).downcase == '.parquet'
    
    begin
      @df = Polars.read_parquet(file)
      @original_headers = @df.columns
      @display_headers  = @original_headers.dup
      @total_rows = @df.height
      @current_offset = 0

      @column_list_proxy.cell_rows = @original_headers.map { |h| [h] }
      
      redraw_main_table_structure
      update_table_view_data
      
    rescue => e
      msg_box_error("Error Reading File", "An error occurred: #{e.message}")
      @df = nil
    end
  end

  def handle_column_selection(selection_index)
    return if @df.nil? || selection_index.nil? || selection_index < 0
    selected_column = @original_headers[selection_index]
    
    @display_headers = [selected_column] + (@original_headers - [selected_column])
    
    redraw_main_table_structure
    update_table_view_data
  end

  def redraw_main_table_structure
    return if @display_headers.empty?
    @main_table_container.children.to_a.each(&:destroy)
    @main_table_container.content {
      @main_table_proxy = table {
        stretchy true
        editable false
        @display_headers.each { |header| text_column(header) }
      }
    }
  end

  def update_table_view_data
    return unless @df && @main_table_proxy
    page_df = @df.slice(@current_offset, PAGE_SIZE)
    page_data = page_df.to_a.map { |row_hash| row_hash.values_at(*@display_headers) }
    @main_table_proxy.cell_rows = page_data
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
    return unless @df && (@current_offset + PAGE_SIZE < @total_rows)
    # Only redraw the table structure if the columns have been reordered.
    if @display_headers != @original_headers
      @display_headers = @original_headers.dup
      redraw_main_table_structure
    end
    @current_offset += PAGE_SIZE
    update_table_view_data
  end

  def go_previous
    return unless @df && @current_offset > 0
    # Revert to original column order if needed.
    if @display_headers != @original_headers
      @display_headers = @original_headers.dup
      redraw_main_table_structure
    end
    @current_offset -= PAGE_SIZE
    @current_offset = [@current_offset, 0].max
    update_table_view_data
  end
end

ParquetViewer.new.launch
