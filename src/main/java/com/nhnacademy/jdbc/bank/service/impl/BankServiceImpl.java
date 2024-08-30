package com.nhnacademy.jdbc.bank.service.impl;

import com.nhnacademy.jdbc.bank.domain.Account;
import com.nhnacademy.jdbc.bank.exception.AccountAreadyExistException;
import com.nhnacademy.jdbc.bank.exception.AccountNotFoundException;
import com.nhnacademy.jdbc.bank.exception.BalanceNotEnoughException;
import com.nhnacademy.jdbc.bank.repository.AccountRepository;
import com.nhnacademy.jdbc.bank.repository.impl.AccountRepositoryImpl;
import com.nhnacademy.jdbc.bank.service.BankService;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Optional;

public class BankServiceImpl implements BankService {

    private final AccountRepository accountRepository;

    public BankServiceImpl(AccountRepository accountRepository) {
        this.accountRepository = accountRepository;
    }

    @Override
    public Account getAccount(Connection connection, long accountNumber){
        //todo#11 계좌-조회
        Optional<Account> account = accountRepository.findByAccountNumber(connection, accountNumber);
        if (account.isEmpty()) {
            throw new AccountNotFoundException(accountNumber);
        }
       return account.get();
    }

    @Override
    public void createAccount(Connection connection, Account account){
        //todo#12 계좌-등록
        // countByAccountNumber, save
        // 이미 있다면,
        if (isExistAccount(connection, account.getAccountNumber())) {
            throw new AccountAreadyExistException(account.getAccountNumber());
        }
        accountRepository.save(connection, account);
    }

    @Override
    public boolean depositAccount(Connection connection, long accountNumber, long amount){
        //todo#13 예금, 계좌가 존재하는지 체크 -> 예금실행 -> 성공 true, 실패 false;
        if (isExistAccount(connection, accountNumber)) {
            accountRepository.deposit(connection, accountNumber, amount);
            return true;
        } else {
            throw new AccountNotFoundException(accountNumber);
        }
    }

    @Override
    public boolean withdrawAccount(Connection connection, long accountNumber, long amount){
        //todo#14 출금, 계좌가 존재하는지 체크 ->  출금가능여부 체크 -> 출금실행, 성공 true, 실폐 false 반환

        if (!isExistAccount(connection, accountNumber)) {
            throw new AccountNotFoundException(accountNumber);
        }

        Optional<Account> accountOptional = accountRepository.findByAccountNumber(connection, accountNumber);
        if (!accountOptional.isEmpty()) {
            int ret = accountRepository.withdraw(connection, accountNumber, amount);
            if (ret < 1) {
                throw new BalanceNotEnoughException(accountNumber);
            }
            return true;
        }

        return false;
    }

    @Override
    public void transferAmount(Connection connection, long accountNumberFrom, long accountNumberTo, long amount){
        //todo#15 계좌 이체 accountNumberFrom -> accountNumberTo 으로 amount만큼 이체
        if (!isExistAccount(connection, accountNumberFrom)) {
            throw new AccountNotFoundException(accountNumberFrom);
        } else if (!isExistAccount(connection, accountNumberTo)) {
            throw new AccountNotFoundException(accountNumberTo);
        } else {
            Optional<Account> accountOptionalFrom = accountRepository.findByAccountNumber(connection, accountNumberFrom);
            Optional<Account> accountOptionalTo = accountRepository.findByAccountNumber(connection, accountNumberTo);

            if (!accountOptionalFrom.isEmpty()) {
                int ret = accountRepository.withdraw(connection, accountNumberFrom, amount);
                if (ret < 1) {
                    throw new BalanceNotEnoughException(accountNumberFrom);
                }
                if (!accountOptionalTo.isEmpty()) {
                    accountRepository.deposit(connection, accountNumberTo, amount);
                } else {
                    throw new AccountNotFoundException(accountNumberTo);
                }
            } else {
                throw new AccountNotFoundException(accountNumberFrom);
            }
        }
    }

    @Override
    public boolean isExistAccount(Connection connection, long accountNumber){
        //todo#16 Account가 존재하면 true , 존재하지 않다면 false
        return accountRepository.countByAccountNumber(connection, accountNumber) > 0;
    }

    @Override
    public void dropAccount(Connection connection, long accountNumber) {
        //todo#17 account 삭제
        if (!isExistAccount(connection, accountNumber)) {
            throw new AccountNotFoundException(accountNumber);
        }

        int ret = accountRepository.deleteByAccountNumber(connection, accountNumber);
        if (ret < 1) {
            throw new RuntimeException();
        }
        
    }

}