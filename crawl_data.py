from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
from datetime import datetime, timedelta
import os
import pandas as pd
from collections import defaultdict
exchanges = [[1,"HOSE"],[2, "HNX"], [3, "UPCoM"], [4, "VN30"], [5, "HNX30"]]
output_file = "thong_ke_dat_lenh.csv"

service = Service(executable_path="../chromedriver-win64/chromedriver-win64/chromedriver.exe")
options = Options()
driver = webdriver.Chrome(service=service, options=options)
wait = WebDriverWait(driver, 20)

check_login = False
headers = None

start_date = datetime.strptime("1/01/2025", "%d/%m/%Y")
end_date = datetime.now()
for exchange_id, exchange_name in exchanges:
    print(f"\n🟢 Đang xử lý sàn: {exchange_name}")
    url = f"https://finance.vietstock.vn/ket-qua-giao-dich?tab=thong-ke-lenh&exchange={exchange_id}"
    driver.get(url)

    wait.until(EC.presence_of_element_located((By.ID, "btn-page-next")))

    # Đăng nhập 1 lần
    if not check_login:
        try:
            next_button = wait.until(EC.element_to_be_clickable((By.ID, "btn-page-next")))
            driver.execute_script("arguments[0].click();", next_button)

            username_field = wait.until(EC.presence_of_element_located((By.NAME, "Email")))
            password_field = driver.find_element(By.NAME, "Password")
            login_button = driver.find_element(By.ID, "btnLoginAccount")

            username_field.send_keys("uyle3614@gmail.com")
            password_field.send_keys("Uyphong0154V")
            login_button.click()
            print("✅ Đăng nhập thành công")
            time.sleep(3)
            check_login = True
        except Exception as e:
            print(f"❌ Lỗi đăng nhập: {e}")
            continue  # Bỏ qua sàn này nếu lỗi đăng nhập

    current_date = start_date
    while current_date <= end_date:
        formatted_date = current_date.strftime("%d/%m/%Y")
        print(f"\n🔍 Ngày: {formatted_date}")
        try:
            driver.refresh()

            # Nên đợi một phần tử "ổn định" xuất hiện lại sau refresh (như tiêu đề trang hoặc vùng chính)
            wait.until(EC.presence_of_element_located((By.ID, "statistic-price")))
            print(4444)
            # Sau đó mới tiếp tục thao tác
            to_input = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "#txtToDate input")))
            to_input.clear()
            to_input.send_keys(formatted_date)
            print(3333)
            xem_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[text()='Xem']")))
            xem_button.click()
            time.sleep(1)

            final_data = []
            print(121212)
            while True:
                table = wait.until(EC.presence_of_element_located((By.ID, "statistic-price")))
                rows = table.find_elements(By.TAG_NAME, "tr")
                print(1111)
                # Kiểm tra có dữ liệu ngày không
                found_date = any(formatted_date in td.text for tr in rows for td in tr.find_elements(By.TAG_NAME, "td"))
                if not found_date:
                    print(f"⚠️ Không có dữ liệu ngày {formatted_date}")
                    break
                if headers is None:
                    header_row = table.find_element(By.TAG_NAME, "thead").find_elements(By.TAG_NAME, "th")
                    headers = [th.text.strip() for th in header_row]
                    print(headers)
                    headers = headers[0:-8:1]
                    headers.insert(0, 'sàn')

                    # Cập nhật tiêu đề cột cho GD Khớp lệnh, GD thỏa thuận và Tổng giao dịch
                    for i, header in enumerate(headers):
                        if header in ["Giá mua tốt nhất", "Giá bán tốt nhất"]:
                            headers[i] = header + " KL"  # Cột KL
                            headers.insert(i + 1, header + " GT")  # Cột GT
                        if header in ["Số lệnh", "Khối lượng"]:
                            headers[i] = header + "mua"  # Cột KL
                            headers.insert(i + 1, header + "bán")  # Cột GT
                            headers.insert(i + 2, header + "Mua-bán")  # Cột GT
                # Lấy dữ liệu từng hàng
                for row in rows:
                    cols = row.find_elements(By.TAG_NAME, "td")

                    if cols:
                        row_data = [col.text.strip() for col in cols]

                        # Xử lý các cột GD Khớp lệnh, GD thỏa thuận và Tổng giao dịch để chia thành KL và GT
                        for i, header in enumerate(headers):
                            if header.endswith("KL"):
                                parent_column = header.replace(" KL", "")  # Lấy tên cột cha (Ví dụ: "GD Khớp lệnh")
                                if parent_column in row_data:
                                    row_data[i] = row_data[row_data.index(parent_column) + 1]
                            elif header.endswith("GT"):
                                parent_column = header.replace(" GT", "")  # Lấy tên cột cha (Ví dụ: "GD Khớp lệnh")
                                if parent_column in row_data:
                                    row_data[i] = row_data[row_data.index(parent_column) + 2]

                        # Thêm dòng dữ liệu vào list
                        final_data.append(row_data)
                # Kiểm tra nút next
                try:
                    next_button = wait.until(EC.presence_of_element_located((By.ID, "btn-page-next")))
                    if next_button.get_attribute("disabled"):
                        break
                    driver.execute_script("arguments[0].click();", next_button)
                    time.sleep(1)
                except:
                    break

            # Ghi dữ liệu ra file
            if final_data:
                final_data = [[exchange_name] + item for item in final_data]
                df_new = pd.DataFrame(final_data, columns=headers)
                if os.path.exists(output_file):
                    df_existing = pd.read_csv(output_file)
                    df_combined = pd.concat([df_existing, df_new], ignore_index=True)
                else:
                    df_combined = df_new
                df_combined.to_csv(output_file, index=False, encoding='utf-8-sig')
                print(f"✅ Đã ghi dữ liệu ngày {formatted_date} vào {output_file}")

        except Exception as e:
            print(f"❌ Lỗi xử lý ngày {formatted_date}: {e}")
        current_date += timedelta(days=1)

driver.quit()
print("\n✅ Hoàn thành tất cả dữ liệu.")
